{-# LANGUAGE OverloadedStrings #-}

-----------------------------------------------------------------------------
-- |
-- Module with consumer properties types and functions.
-----------------------------------------------------------------------------
module Kafka.Consumer.ConsumerProperties
( ConsumerProperties(..)
, CallbackPollMode(..)
, brokersList
, autoCommit
, noAutoCommit
, noAutoOffsetStore
, groupId
, clientId
, setCallback
, logLevel
, compression
, suppressDisconnectLogs
, statisticsInterval
, extraProps
, extraProp
, debugOptions
, queuedMaxMessagesKBytes
, callbackPollMode
, module X
)
where

import           Control.Monad        (MonadPlus (mplus))
import           Data.Map             (Map)
import qualified Data.Map             as M
import           Data.Semigroup       as Sem
import           Data.Text            (Text)
import qualified Data.Text            as Text
import           Kafka.Consumer.Types (ConsumerGroupId (..))
import           Kafka.Internal.Setup (KafkaConf (..), Callback(..))
import           Kafka.Types          (BrokerAddress (..), ClientId (..), KafkaCompressionCodec (..), KafkaDebug (..), KafkaLogLevel (..), Millis (..), kafkaCompressionCodecToText, kafkaDebugToText)

import Kafka.Consumer.Callbacks as X

-- | Whether the callback polling should be done synchronously or not.
data CallbackPollMode =
    -- | You have to poll the consumer frequently to handle new messages
    -- as well as rebalance and keep alive events.
    -- This enables lowering the footprint and having full control over when polling
    -- happens, at the cost of manually managing those events.
    CallbackPollModeSync
    -- | Handle polling rebalance and keep alive events for you in a background thread.
  | CallbackPollModeAsync deriving (Show, Eq)

-- | Properties to create 'Kafka.Consumer.Types.KafkaConsumer'.
data ConsumerProperties = ConsumerProperties
  { cpProps            :: Map Text Text
  , cpLogLevel         :: Maybe KafkaLogLevel
  , cpCallbacks        :: [Callback]
  , cpCallbackPollMode :: CallbackPollMode
  }

instance Sem.Semigroup ConsumerProperties where
  (ConsumerProperties m1 ll1 cb1 _) <> (ConsumerProperties m2 ll2 cb2 cup2) =
    ConsumerProperties (M.union m2 m1) (ll2 `mplus` ll1) (cb1 `mplus` cb2) cup2
  {-# INLINE (<>) #-}

-- | /Right biased/ so we prefer newer properties over older ones.
instance Monoid ConsumerProperties where
  mempty = ConsumerProperties
    { cpProps             = M.empty
    , cpLogLevel          = Nothing
    , cpCallbacks         = []
    , cpCallbackPollMode  = CallbackPollModeAsync
    }
  {-# INLINE mempty #-}
  mappend = (Sem.<>)
  {-# INLINE mappend #-}

-- | Set the <https://kafka.apache.org/documentation/#bootstrap.servers list of brokers> to contact to connect to the Kafka cluster.
brokersList :: [BrokerAddress] -> ConsumerProperties
brokersList bs =
  let bs' = Text.intercalate "," (unBrokerAddress <$> bs)
   in extraProps $ M.fromList [("bootstrap.servers", bs')]

-- | Set the <https://kafka.apache.org/documentation/#auto.commit.interval.ms auto commit interval> and enables <https://kafka.apache.org/documentation/#enable.auto.commit auto commit>.
autoCommit :: Millis -> ConsumerProperties
autoCommit (Millis ms) = extraProps $
  M.fromList
    [ ("enable.auto.commit", "true")
    , ("auto.commit.interval.ms", Text.pack $ show ms)
    ]

-- | Disable <https://kafka.apache.org/documentation/#enable.auto.commit auto commit> for the consumer.
noAutoCommit :: ConsumerProperties
noAutoCommit =
  extraProps $ M.fromList [("enable.auto.commit", "false")]

-- | Disable auto offset store for the consumer.
--
-- See <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md enable.auto.offset.store> for more information.
noAutoOffsetStore :: ConsumerProperties
noAutoOffsetStore =
  extraProps $ M.fromList [("enable.auto.offset.store", "false")]

-- | Set the consumer <https://kafka.apache.org/documentation/#group.id group id>.
groupId :: ConsumerGroupId -> ConsumerProperties
groupId (ConsumerGroupId cid) =
  extraProps $ M.fromList [("group.id", cid)]

-- | Set the <https://kafka.apache.org/documentation/#client.id consumer identifier>.
clientId :: ClientId -> ConsumerProperties
clientId (ClientId cid) =
  extraProps $ M.fromList [("client.id", cid)]

-- | Set the consumer callback.
--
-- For examples of use, see:
--
-- * 'errorCallback'
-- * 'logCallback'
-- * 'statsCallback'
setCallback :: Callback -> ConsumerProperties
setCallback cb = mempty { cpCallbacks = [cb] }

-- | Set the logging level.
-- Usually is used with 'debugOptions' to configure which logs are needed.
logLevel :: KafkaLogLevel -> ConsumerProperties
logLevel ll = mempty { cpLogLevel = Just ll }

-- | Set the <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md compression.codec> for the consumer.
compression :: KafkaCompressionCodec -> ConsumerProperties
compression c =
  extraProps $ M.singleton "compression.codec" (kafkaCompressionCodecToText c)

-- | Suppresses consumer <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md log.connection.close>.
--
-- It might be useful to turn this off when interacting with brokers
-- with an aggressive @connection.max.idle.ms@ value.
suppressDisconnectLogs :: ConsumerProperties
suppressDisconnectLogs =
  extraProps $ M.fromList [("log.connection.close", "false")]

-- | Set the <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md statistics.interval.ms> for the producer.
statisticsInterval :: Millis -> ConsumerProperties
statisticsInterval (Millis t) =
  extraProps $ M.singleton "statistics.interval.ms" (Text.pack $ show t)

-- | Set any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProps :: Map Text Text -> ConsumerProperties
extraProps m = mempty { cpProps = m }
{-# INLINE extraProps #-}

-- | Set any configuration option that is supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProp :: Text -> Text -> ConsumerProperties
extraProp k v = mempty { cpProps = M.singleton k v }
{-# INLINE extraProp #-}

-- | Set <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md debug> features for the consumer.
-- Usually is used with 'logLevel'.
debugOptions :: [KafkaDebug] -> ConsumerProperties
debugOptions [] = extraProps M.empty
debugOptions d =
  let points = Text.intercalate "," (kafkaDebugToText <$> d)
   in extraProps $ M.fromList [("debug", points)]

-- | Set <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md queued.max.messages.kbytes>
queuedMaxMessagesKBytes :: Int -> ConsumerProperties
queuedMaxMessagesKBytes kBytes =
  extraProp "queued.max.messages.kbytes" (Text.pack $ show kBytes)
{-# INLINE queuedMaxMessagesKBytes #-}

-- | Set the callback poll mode. Default value is 'CallbackPollModeAsync'.
-- Since the Semigroup instance for 'ConsumerProperties' is right-biased, this option
-- __has__ to be set last or it will be ignored.
callbackPollMode :: CallbackPollMode -> ConsumerProperties
callbackPollMode mode = mempty { cpCallbackPollMode = mode }
