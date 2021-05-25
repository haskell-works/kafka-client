{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

-----------------------------------------------------------------------------
-- |
-- Module to consume messages from Kafka topics.
-- 
-- Here's an example of code to consume messages from a topic:
-- 
-- @
-- import Control.Exception (bracket)
-- import Control.Monad (replicateM_)
-- import Kafka.Consumer
-- 
-- -- Global consumer properties
-- consumerProps :: 'ConsumerProperties'
-- consumerProps = 'brokersList' ["localhost:9092"]
--              <> 'groupId' ('ConsumerGroupId' "consumer_example_group")
--              <> 'noAutoCommit'
--              <> 'logLevel' 'KafkaLogInfo'
-- 
-- -- Subscription to topics
-- consumerSub :: 'Subscription'
-- consumerSub = 'topics' ['TopicName' "kafka-client-example-topic"]
--            <> 'offsetReset' 'Earliest'
-- 
-- -- Running an example
-- runConsumerExample :: IO ()
-- runConsumerExample = do
--     res <- bracket mkConsumer clConsumer runHandler
--     print res
--     where
--       mkConsumer = 'newConsumer' consumerProps consumerSub
--       clConsumer (Left err) = pure (Left err)
--       clConsumer (Right kc) = (maybe (Right ()) Left) \<$\> 'closeConsumer' kc
--       runHandler (Left err) = pure (Left err)
--       runHandler (Right kc) = processMessages kc
-- 
-- -- Example polling 10 times before stopping
-- processMessages :: 'KafkaConsumer' -> IO (Either 'KafkaError' ())
-- processMessages kafka = do
--     replicateM_ 10 $ do
--       msg <- 'pollMessage' kafka ('Timeout' 1000)
--       putStrLn $ "Message: " <> show msg
--       err <- 'commitAllOffsets' 'OffsetCommit' kafka
--       putStrLn $ "Offsets: " <> maybe "Committed." show err
--     pure $ Right ()
-- @
-----------------------------------------------------------------------------
module Kafka.Consumer
( KafkaConsumer
, module X
, runConsumer
, newConsumer
, assign, assignment, subscription
, pausePartitions, resumePartitions
, committed, position, seek
, pollMessage, pollConsumerEvents
, pollMessageBatch
, commitOffsetMessage, commitAllOffsets, commitPartitionsOffsets
, storeOffsets, storeOffsetMessage
, closeConsumer
-- ReExport Types
, RdKafkaRespErrT (..)
)
where

import           Control.Arrow              (left, (&&&))
import           Control.Concurrent         (forkIO, modifyMVar, rtsSupportsBoundThreads, withMVar)
import           Control.Exception          (bracket)
import           Control.Monad              (forM_, void, when)
import           Control.Monad.IO.Class     (MonadIO (liftIO))
import           Control.Monad.Trans.Except (ExceptT (ExceptT), runExceptT)
import           Data.Bifunctor             (bimap, first)
import qualified Data.ByteString            as BS
import           Data.IORef                 (readIORef, writeIORef)
import qualified Data.Map                   as M
import           Data.Maybe                 (fromMaybe)
import           Data.Monoid                ((<>))
import           Data.Set                   (Set)
import qualified Data.Set                   as Set
import qualified Data.Text                  as Text
import           Foreign                    hiding (void)
import           Kafka.Consumer.Convert     (fromMessagePtr, fromNativeTopicPartitionList'', offsetCommitToBool, offsetToInt64, toMap, toNativeTopicPartitionList, toNativeTopicPartitionList', toNativeTopicPartitionListNoDispose, topicPartitionFromMessageForCommit)
import           Kafka.Consumer.Types       (KafkaConsumer (..))
import           Kafka.Internal.RdKafka     (RdKafkaRespErrT (..), RdKafkaTopicPartitionListTPtr, RdKafkaTypeT (..), newRdKafkaT, newRdKafkaTopicPartitionListT, newRdKafkaTopicT, rdKafkaAssign, rdKafkaAssignment, rdKafkaCommit, rdKafkaCommitted, rdKafkaConfSetDefaultTopicConf, rdKafkaConsumeBatchQueue, rdKafkaConsumeQueue, rdKafkaConsumerClose, rdKafkaConsumerPoll, rdKafkaOffsetsStore, rdKafkaPausePartitions, rdKafkaPollSetConsumer, rdKafkaPosition, rdKafkaQueueDestroy, rdKafkaQueueNew, rdKafkaResumePartitions, rdKafkaSeek, rdKafkaSetLogLevel, rdKafkaSubscribe, rdKafkaSubscription, rdKafkaTopicConfDup, rdKafkaTopicPartitionListAdd)
import           Kafka.Internal.Setup       (CallbackPollStatus (..), Kafka (..), KafkaConf (..), KafkaProps (..), TopicConf (..), TopicProps (..), getKafkaConf, getRdKafka, kafkaConf, topicConf, Callback(..))
import           Kafka.Internal.Shared      (kafkaErrorToMaybe, maybeToLeft, rdKafkaErrorToEither)

import Kafka.Consumer.ConsumerProperties as X
import Kafka.Consumer.Subscription       as X
import Kafka.Consumer.Types              as X hiding (KafkaConsumer)
import Kafka.Types                       as X

-- | Runs high-level kafka consumer.
-- A callback provided is expected to call 'pollMessage' when convenient.
{-# DEPRECATED runConsumer "Use 'newConsumer'/'closeConsumer' instead" #-}
runConsumer :: ConsumerProperties
            -> Subscription
            -> (KafkaConsumer -> IO (Either KafkaError a))  -- ^ A callback function to poll and handle messages
            -> IO (Either KafkaError a)
runConsumer cp sub f =
  bracket mkConsumer clConsumer runHandler
  where
    mkConsumer = newConsumer cp sub

    clConsumer (Left err) = return (Left err)
    clConsumer (Right kc) = maybeToLeft <$> closeConsumer kc

    runHandler (Left err) = return (Left err)
    runHandler (Right kc) = f kc

-- | Create a `KafkaConsumer`. This consumer must be correctly released using 'closeConsumer'.
newConsumer :: MonadIO m
            => ConsumerProperties
            -> Subscription
            -> m (Either KafkaError KafkaConsumer)
newConsumer props (Subscription ts tp) = liftIO $ do
  let cp = case cpCallbackPollMode props of
            CallbackPollModeAsync -> setCallback (rebalanceCallback (\_ _ -> return ())) <> props
            CallbackPollModeSync  -> props
  kc@(KafkaConf kc' qref _) <- newConsumerConf cp
  tp' <- topicConf (TopicProps tp)
  _   <- setDefaultTopicConf kc tp'
  rdk <- newRdKafkaT RdKafkaConsumer kc'
  case rdk of
    Left err   -> return . Left $ KafkaError err
    Right rdk' -> do
      when (cpCallbackPollMode props == CallbackPollModeAsync) $ do
        msgq <- rdKafkaQueueNew rdk'
        writeIORef qref (Just msgq)
      let kafka = KafkaConsumer (Kafka rdk') kc
      redErr <- redirectCallbacksPoll kafka
      case redErr of
        Just err -> closeConsumer kafka >> return (Left err)
        Nothing  -> do
          forM_ (cpLogLevel cp) (setConsumerLogLevel kafka)
          sub <- subscribe kafka ts
          case sub of
            Nothing  -> (when (cpCallbackPollMode props == CallbackPollModeAsync) $
              runConsumerLoop kafka (Just $ Timeout 100)) >> return (Right kafka)
            Just err -> closeConsumer kafka >> return (Left err)

-- | Polls a single message
pollMessage :: MonadIO m
            => KafkaConsumer
            -> Timeout -- ^ the timeout, in milliseconds
            -> m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString))) -- ^ Left on error or timeout, right for success
pollMessage c@(KafkaConsumer _ (KafkaConf _ qr _)) (Timeout ms) = liftIO $ do
  mbq <- readIORef qr
  case mbq of
    Nothing -> rdKafkaConsumerPoll (getRdKafka c) ms >>= fromMessagePtr
    Just q  -> rdKafkaConsumeQueue q (fromIntegral ms) >>= fromMessagePtr

-- | Polls up to 'BatchSize' messages.
-- Unlike 'pollMessage' this function does not return usual "timeout" errors.
-- An empty batch is returned when there are no messages available.
--
-- This API is not available when 'CallbackPollMode' is set to 'CallbackPollModeSync'.
pollMessageBatch :: MonadIO m
                 => KafkaConsumer
                 -> Timeout
                 -> BatchSize
                 -> m [Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString))]
pollMessageBatch c@(KafkaConsumer _ (KafkaConf _ qr _)) (Timeout ms) (BatchSize b) = liftIO $ do
  pollConsumerEvents c Nothing
  mbq <- readIORef qr
  case mbq of
    Nothing -> return [Left $ KafkaBadSpecification "Calling pollMessageBatch while CallbackPollMode is set to CallbackPollModeSync."]
    Just q  -> whileNoCallbackRunning c $ rdKafkaConsumeBatchQueue q ms b >>= traverse fromMessagePtr

-- | Commit message's offset on broker for the message's partition.
commitOffsetMessage :: MonadIO m
                    => OffsetCommit
                    -> KafkaConsumer
                    -> ConsumerRecord k v
                    -> m (Maybe KafkaError)
commitOffsetMessage o k m =
  liftIO $ toNativeTopicPartitionList [topicPartitionFromMessageForCommit m] >>= commitOffsets o k

-- | Stores message's offset locally for the message's partition.
storeOffsetMessage :: MonadIO m
                   => KafkaConsumer
                   -> ConsumerRecord k v
                   -> m (Maybe KafkaError)
storeOffsetMessage k m =
  liftIO $ toNativeTopicPartitionListNoDispose [topicPartitionFromMessageForCommit m] >>= commitOffsetsStore k

-- | Stores offsets locally
storeOffsets :: MonadIO m
             => KafkaConsumer
             -> [TopicPartition]
             -> m (Maybe KafkaError)
storeOffsets k ps =
  liftIO $ toNativeTopicPartitionListNoDispose ps >>= commitOffsetsStore k

-- | Commit offsets for all currently assigned partitions.
commitAllOffsets :: MonadIO m
                 => OffsetCommit
                 -> KafkaConsumer
                 -> m (Maybe KafkaError)
commitAllOffsets o k =
  liftIO $ newForeignPtr_ nullPtr >>= commitOffsets o k

-- | Commit offsets for all currently assigned partitions.
commitPartitionsOffsets :: MonadIO m
                 => OffsetCommit
                 -> KafkaConsumer
                 -> [TopicPartition]
                 -> m (Maybe KafkaError)
commitPartitionsOffsets o k ps =
  liftIO $ toNativeTopicPartitionList ps >>= commitOffsets o k

-- | Assigns the consumer to consume from the given topics, partitions,
-- and offsets.
assign :: MonadIO m => KafkaConsumer -> [TopicPartition] -> m (Maybe KafkaError)
assign (KafkaConsumer (Kafka k) _) ps = liftIO $ do
  tps <- toNativeTopicPartitionList ps
  kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaAssign k tps

-- | Returns current consumer's assignment
assignment :: MonadIO m => KafkaConsumer -> m (Either KafkaError (M.Map TopicName [PartitionId]))
assignment (KafkaConsumer (Kafka k) _) = liftIO $ do
  tpl <- rdKafkaAssignment k
  tps <- traverse fromNativeTopicPartitionList'' (left KafkaResponseError tpl)
  return $ tpMap <$> tps
  where
    tpMap ts = toMap $ (tpTopicName &&& tpPartition) <$> ts

-- | Returns current consumer's subscription
subscription :: MonadIO m => KafkaConsumer -> m (Either KafkaError [(TopicName, SubscribedPartitions)])
subscription (KafkaConsumer (Kafka k) _) = liftIO $ do
  tpl <- rdKafkaSubscription k
  tps <- traverse fromNativeTopicPartitionList'' (left KafkaResponseError tpl)
  return $ toSub <$> tps
  where
    toSub ts = M.toList $ subParts <$> tpMap ts
    tpMap ts = toMap $ (tpTopicName &&& tpPartition) <$> ts
    subParts [PartitionId (-1)] = SubscribedPartitionsAll
    subParts ps                 = SubscribedPartitions ps

-- | Pauses specified partitions on the current consumer.
pausePartitions :: MonadIO m => KafkaConsumer -> [(TopicName, PartitionId)] -> m KafkaError
pausePartitions (KafkaConsumer (Kafka k) _) ps = liftIO $ do
  pl <- newRdKafkaTopicPartitionListT (length ps)
  mapM_ (\(TopicName topicName, PartitionId partitionId) -> rdKafkaTopicPartitionListAdd pl (Text.unpack topicName) partitionId) ps
  KafkaResponseError <$> rdKafkaPausePartitions k pl

-- | Resumes specified partitions on the current consumer.
resumePartitions :: MonadIO m => KafkaConsumer -> [(TopicName, PartitionId)] -> m KafkaError
resumePartitions (KafkaConsumer (Kafka k) _) ps = liftIO $ do
  pl <- newRdKafkaTopicPartitionListT (length ps)
  mapM_ (\(TopicName topicName, PartitionId partitionId) -> rdKafkaTopicPartitionListAdd pl (Text.unpack topicName) partitionId) ps
  KafkaResponseError <$> rdKafkaResumePartitions k pl

-- | Seek a particular offset for each provided 'TopicPartition'
seek :: MonadIO m => KafkaConsumer -> Timeout -> [TopicPartition] -> m (Maybe KafkaError)
seek (KafkaConsumer (Kafka k) _) (Timeout timeout) tps = liftIO $
  either Just (const Nothing) <$> seekAll
  where
    seekAll = runExceptT $ do
      tr <- traverse (ExceptT . topicPair) tps
      mapM_ (\(kt, p, o) -> ExceptT (rdSeek kt p o)) tr

    rdSeek kt (PartitionId p) o =
      rdKafkaErrorToEither <$> rdKafkaSeek kt (fromIntegral p) (offsetToInt64 o) timeout

    topicPair tp = do
      let (TopicName tn) = tpTopicName tp
      nt <- newRdKafkaTopicT k (Text.unpack tn) Nothing
      return $ bimap KafkaError (,tpPartition tp, tpOffset tp) (first Text.pack nt)

-- | Retrieve committed offsets for topics+partitions.
committed :: MonadIO m => KafkaConsumer -> Timeout -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
committed (KafkaConsumer (Kafka k) _) (Timeout timeout) tps = liftIO $ do
  ntps <- toNativeTopicPartitionList' tps
  res <- rdKafkaCommitted k ntps timeout
  case res of
    RdKafkaRespErrNoError -> Right <$> fromNativeTopicPartitionList'' ntps
    err                   -> return $ Left (KafkaResponseError err)

-- | Retrieve current positions (last consumed message offset+1) for the current running instance of the consumer.
-- If the current consumer hasn't received any messages for a given partition, 'PartitionOffsetInvalid' is returned.
position :: MonadIO m => KafkaConsumer -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
position (KafkaConsumer (Kafka k) _) tps = liftIO $ do
  ntps <- toNativeTopicPartitionList' tps
  res <- rdKafkaPosition k ntps
  case res of
    RdKafkaRespErrNoError -> Right <$> fromNativeTopicPartitionList'' ntps
    err                   -> return $ Left (KafkaResponseError err)

-- | Polls the provided kafka consumer for events.
--
-- Events will cause application provided callbacks to be called.
--
-- The 'Timeout' argument specifies the maximum amount of time
-- (in milliseconds) that the call will block waiting for events.
--
-- This function is called on each 'pollMessage' and, if runtime allows
-- multi threading, it is called periodically in a separate thread
-- to ensure the callbacks are handled ASAP.
--
-- There is no particular need to call this function manually
-- unless some special cases in a single-threaded environment
-- when polling for events on each 'pollMessage' is not
-- frequent enough.
pollConsumerEvents :: KafkaConsumer -> Maybe Timeout -> IO ()
pollConsumerEvents k timeout =
  void . withCallbackPollEnabled k $ pollConsumerEvents' k timeout

-- | Closes the consumer.
-- 
-- See 'newConsumer'
closeConsumer :: MonadIO m => KafkaConsumer -> m (Maybe KafkaError)
closeConsumer (KafkaConsumer (Kafka k) (KafkaConf _ qr statusVar)) = liftIO $
  -- because closing the consumer will raise callbacks,
  -- prevent the async loop from doing it at the same time
  modifyMVar statusVar $ \_ -> do
    -- librdkafka says:
    --   Prior to destroying the client instance, loose your reference to the
    --   background queue by calling rd_kafka_queue_destroy()
    readIORef qr >>= mapM_ rdKafkaQueueDestroy
    res <- kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaConsumerClose k
    pure (CallbackPollDisabled, res)
-----------------------------------------------------------------------------
newConsumerConf :: ConsumerProperties -> IO KafkaConf
newConsumerConf ConsumerProperties {cpProps = m, cpCallbacks = cbs} = do
  conf <- kafkaConf (KafkaProps m)
  forM_ cbs (\(Callback setCb) -> setCb conf)
  return conf

-- | Subscribes to a given list of topics.
--
-- Wildcard (regex) topics are supported by the /librdkafka/ assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
subscribe :: KafkaConsumer -> Set TopicName -> IO (Maybe KafkaError)
subscribe (KafkaConsumer (Kafka k) _) ts = do
    pl <- newRdKafkaTopicPartitionListT (length ts)
    mapM_ (\(TopicName t) -> rdKafkaTopicPartitionListAdd pl (Text.unpack t) (-1)) (Set.toList ts)
    res <- KafkaResponseError <$> rdKafkaSubscribe k pl
    return $ kafkaErrorToMaybe res

setDefaultTopicConf :: KafkaConf -> TopicConf -> IO ()
setDefaultTopicConf (KafkaConf kc _ _) (TopicConf tc) =
    rdKafkaTopicConfDup tc >>= rdKafkaConfSetDefaultTopicConf kc

commitOffsets :: OffsetCommit -> KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsets o (KafkaConsumer (Kafka k) _) pl =
    kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaCommit k pl (offsetCommitToBool o)

commitOffsetsStore :: KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsetsStore (KafkaConsumer (Kafka k) _) pl =
    kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaOffsetsStore k pl

setConsumerLogLevel :: KafkaConsumer -> KafkaLogLevel -> IO ()
setConsumerLogLevel (KafkaConsumer (Kafka k) _) level =
  liftIO $ rdKafkaSetLogLevel k (fromEnum level)

redirectCallbacksPoll :: KafkaConsumer -> IO (Maybe KafkaError)
redirectCallbacksPoll (KafkaConsumer (Kafka k) _) =
  kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaPollSetConsumer k

runConsumerLoop :: KafkaConsumer -> Maybe Timeout -> IO ()
runConsumerLoop k timeout =
  when rtsSupportsBoundThreads $ void $ forkIO go
  where
    go = do
      st <- withCallbackPollEnabled k (pollConsumerEvents' k timeout)
      case st of
        CallbackPollEnabled  -> go
        CallbackPollDisabled -> pure ()

whileNoCallbackRunning :: KafkaConsumer -> IO a -> IO a
whileNoCallbackRunning k f = do
  let statusVar = kcfgCallbackPollStatus (getKafkaConf k)
  withMVar statusVar $ \_ -> f

withCallbackPollEnabled :: KafkaConsumer -> IO () -> IO CallbackPollStatus
withCallbackPollEnabled k f = do
  let statusVar = kcfgCallbackPollStatus (getKafkaConf k)
  withMVar statusVar $ \case
   CallbackPollEnabled  -> f >> pure CallbackPollEnabled
   CallbackPollDisabled -> pure CallbackPollDisabled

pollConsumerEvents' :: KafkaConsumer -> Maybe Timeout -> IO ()
pollConsumerEvents' k timeout =
  let (Timeout tm) = fromMaybe (Timeout 0) timeout
  in void $ rdKafkaConsumerPoll (getRdKafka k) tm
