module Kafka.Consumer
( runConsumerConf
, runConsumer
, newKafkaConsumerConf
, newKafkaConsumer
, setRebalanceCallback
, assign
, subscribe
, pollMessage
, commitOffsetMessage
, commitAllOffsets
, setOffsetCommitCallback
, closeConsumer

-- Types
, CIT.ConsumerGroupId (..)
, CIT.TopicName (..)
, CIT.OffsetCommit (..)
, IT.BrokersString (..)
, IT.Kafka
, IT.KafkaError (..)
, CIT.KafkaTopicPartition (..)
, RDE.RdKafkaRespErrT (..)
)
where

import           Control.Exception
import           Foreign
import           Kafka
import           Kafka.Consumer.Internal.Convert
import           Kafka.Consumer.Internal.Types
import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum
import           Kafka.Internal.Setup
import           Kafka.Internal.Shared
import           Kafka.Internal.Types

import qualified Kafka.Consumer.Internal.Types   as CIT
import qualified Kafka.Internal.RdKafkaEnum      as RDE
import qualified Kafka.Internal.Types            as IT

-- | Runs high-level kafka consumer.
--
-- A callback provided is expected to call 'pollMessage' when convenient.
runConsumerConf :: KafkaConf                            -- ^ Consumer config (see 'newKafkaConsumerConf')
                -> BrokersString                        -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
                -> [TopicName]                          -- ^ List of topics to be consumed
                -> (Kafka -> IO (Either KafkaError a))  -- ^ A callback function to poll and handle messages
                -> IO (Either KafkaError a)
runConsumerConf c bs ts f =
    bracket mkConsumer clConsumer runHandler
    where
        mkConsumer = do
            kafka <- newKafkaConsumer bs c
            -- _ <- setHlConsumer kafka
            sErr  <- subscribe kafka ts
            return $ if hasError sErr
                         then Left (sErr, kafka)
                         else Right kafka

        clConsumer (Left (_, kafka)) = kafkaErrorToEither <$> closeConsumer kafka
        clConsumer (Right kafka) = kafkaErrorToEither <$> closeConsumer kafka

        runHandler (Left (err, _)) = return $ Left err
        runHandler (Right kafka) = f kafka

-- | Runs high-level kafka consumer.
--
-- A callback provided is expected to call 'pollMessage' when convenient.
runConsumer :: ConsumerGroupId                       -- ^ Consumer group id (a @group.id@ property of a kafka consumer)
             -> ConfigOverrides                      -- ^ Extra kafka consumer parameters (see kafka documentation)
             -> BrokersString                        -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
             -> [TopicName]                          -- ^ List of topics to be consumed
             -> (Kafka -> IO (Either KafkaError a))  -- ^ A callback function to poll and handle messages
             -> IO (Either KafkaError a)
runConsumer g c bs ts f = do
    conf <- newKafkaConsumerConf g c
    runConsumerConf conf bs ts f

-- | Creates a new kafka configuration for a consumer with a specified 'ConsumerGroupId'.
newKafkaConsumerConf :: ConsumerGroupId  -- ^ Consumer group id (a @group.id@ property of a kafka consumer)
                     -> ConfigOverrides  -- ^ Extra kafka consumer parameters (see kafka documentation)
                     -> IO KafkaConf     -- ^ Kafka configuration which can be altered before it is used in 'newKafkaConsumer'
newKafkaConsumerConf (ConsumerGroupId gid) conf = do
    kc <- kafkaConf conf
    setKafkaConfValue kc "group.id" gid
    return kc

-- | Creates a new kafka consumer
newKafkaConsumer :: BrokersString -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
                 -> KafkaConf     -- ^ Kafka configuration for a consumer (see 'newKafkaConsumerConf')
                 -> IO Kafka      -- ^ Kafka instance
newKafkaConsumer (BrokersString bs) conf = do
    kafka <- newKafkaPtr RdKafkaConsumer conf
    addBrokers kafka bs
    return kafka

-- | Sets a callback that is called when rebalance is needed.
--
-- Callback implementations suppose to watch for 'KafkaResponseError' 'RdKafkaRespErrAssignPartitions' and
-- for 'KafkaResponseError' 'RdKafkaRespErrRevokePartitions'. Other error codes are not expected and would indicate
-- something really bad happening in a system, or bugs in @librdkafka@ itself.
--
-- A callback is expected to call 'assign' according to the error code it receives.
--
--     * When 'RdKafkaRespErrAssignPartitions' happens 'assign' should be called with all the partitions it was called with.
--       It is OK to alter partitions offsets before calling 'assign'.
--
--     * When 'RdKafkaRespErrRevokePartitions' happens 'assign' should be called with an empty list of partitions.
setRebalanceCallback :: KafkaConf
                     -> (Kafka -> KafkaError -> [KafkaTopicPartition] -> IO ())
                     -> IO ()
setRebalanceCallback (KafkaConf conf) callback = rdKafkaConfSetRebalanceCb conf realCb
  where
    realCb :: Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> Ptr Word8 -> IO ()
    realCb rk err pl _ = do
        rk' <- newForeignPtr_ rk
        pl' <- peek pl
        ps  <- fromNativeTopicPartitionList pl'
        callback (Kafka rk' (KafkaConf conf)) (KafkaResponseError err) ps

-- | Sets a callback that is called when rebalance is needed.
--
-- The results of automatic or manual offset commits will be scheduled
-- for this callback and is served by `pollMessage`.
--
-- A callback is expected to call 'assign' according to the error code it receives.
--
-- If no partitions had valid offsets to commit this callback will be called
-- with `KafkaError` == `KafkaResponseError` `RdKafkaRespErrNoOffset` which is not to be considered
-- an error.
setOffsetCommitCallback :: KafkaConf
                        -> (Kafka -> KafkaError -> [KafkaTopicPartition] -> IO ())
                        -> IO ()
setOffsetCommitCallback (KafkaConf conf) callback = rdKafkaConfSetOffsetCommitCb conf realCb
  where
    realCb :: Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> Ptr Word8 -> IO ()
    realCb rk err pl _ = do
        rk' <- newForeignPtr_ rk
        pl' <- peek pl
        ps  <- fromNativeTopicPartitionList pl'
        callback (Kafka rk' (KafkaConf conf)) (KafkaResponseError err) ps

-- | Assigns specified partitions to a current consumer.
-- Assigning an empty list means unassigning from all partitions that are currently assigned.
-- See 'setRebalanceCallback' for more details.
assign :: Kafka -> [KafkaTopicPartition] -> IO KafkaError
assign (Kafka k _) ps =
    let pl = if null ps
                then newForeignPtr_ nullPtr
                else toNativeTopicPartitionList ps
    in  KafkaResponseError <$> (pl >>= rdKafkaAssign k)

-- | Subscribes to a given list of topics.
--
-- Wildcard (regex) topics are supported by the librdkafka assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
subscribe :: Kafka -> [TopicName] -> IO KafkaError
subscribe (Kafka k _) ts = do
    pl <- newRdKafkaTopicPartitionListT (length ts)
    mapM_ (\(TopicName t) -> rdKafkaTopicPartitionListAdd pl t (-1)) ts
    KafkaResponseError <$> rdKafkaSubscribe k pl

-- | Commit message's offset on broker for the message's partition.
commitOffsetMessage :: Kafka                   -- ^ Kafka handle
                    -> OffsetCommit            -- ^ Offset commit mode, will block if `OffsetCommit`
                    -> KafkaMessage
                    -> IO (Maybe KafkaError)
commitOffsetMessage k o m =
    toNativeTopicPartitionList [topicPartitionFromMessage m] >>= commitOffsets k o

-- | Commit offsets for all currently assigned partitions.
commitAllOffsets :: Kafka                      -- ^ Kafka handle
                 -> OffsetCommit               -- ^ Offset commit mode, will block if `OffsetCommit`
                 -> IO (Maybe KafkaError)
commitAllOffsets k o =
    newForeignPtr_ nullPtr >>= commitOffsets k o

-- | Closes the consumer and destroys it.
closeConsumer :: Kafka -> IO KafkaError
closeConsumer (Kafka k _) = KafkaResponseError <$> rdKafkaConsumerClose k

-----------------------------------------------------------------------------
pollMessage :: Kafka
               -> Int -- ^ the timeout, in milliseconds (@10^3@ per second)
               -> IO (Either KafkaError KafkaMessage) -- ^ Left on error or timeout, right for success
pollMessage (Kafka k _) timeout =
    rdKafkaConsumerPoll k (fromIntegral timeout) >>= fromMessagePtr

commitOffsets :: Kafka -> OffsetCommit -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsets (Kafka k _) o pl =
    (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaCommit k pl (offsetCommitToBool o)


-- | Redirects 'consumeMessage' to poll. Implementation details.
-- setHlConsumer :: Kafka -> IO KafkaError
-- setHlConsumer (Kafka k _) = KafkaResponseError <$> rdKafkaPollSetConsumer k

-- | Sets the offset store for a specified topic.
-- @librdkafka@ supports both @broker@ and @file@ but it seems that consumers with groups
-- can only support @broker@. Which is good and enough.
-- setOffsetStore :: KafkaTopic -> OffsetStoreMethod -> IO ()
-- setOffsetStore t o =
--     let setValue = setTopicValue t
--     in  case o of
--           OffsetStoreBroker ->
--               setValue "offset.store.method" "broker"

--           OffsetStoreFile path sync -> do
--               setValue "offset.store.method" "file"
--               setValue "offset.store.file" path
--               setValue "offset.store.sync.interval.ms" (show $ offsetSyncToInt sync)
