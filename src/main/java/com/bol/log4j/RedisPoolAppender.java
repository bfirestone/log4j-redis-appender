/**
 * Based on code by Pavlo Baron, Landro Silva & Ryan Tenney
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.bol.log4j;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;


public class RedisPoolAppender extends AppenderSkeleton {

    // configs
    private String host = "localhost";
    private int port = 6379;
    private String password;
    private String key;
    private boolean purgeOnFailure = true;

    // runtime stuff
    private int messageIndex = 0;
    private String eventMessage;
    private JedisPool jedisPool;

    // metrics
    private int eventCounter = 0;
    private int eventsDroppedInQueueing = 0;
    private int eventsDroppedInPush = 0;
    private int connectCounter = 0;
    private int connectFailures = 0;
    private int batchPurges = 0;
    private int eventsPushed = 0;


    @Override
    protected void append(LoggingEvent event) {
        jedisPool = new JedisPool(new JedisPoolConfig(), host, port);

        populateEvent(event);
        eventMessage = layout.format(event);
        publish();
    }

    protected void populateEvent(LoggingEvent event) {
        event.getThreadName();
        event.getRenderedMessage();
        event.getNDC();
        event.getMDCCopy();
        event.getThrowableStrRep();
        event.getLocationInformation();
    }

    @Override
    public void close() {

    }

    private boolean publish() {
        LogLog.debug("Sending " + messageIndex + " log messages to Redis at " + getRedisAddress());

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.publish(key, eventMessage);
            eventsPushed += messageIndex;
            messageIndex = 0;
            jedisPool.destroy();
            return true;
        } catch (JedisDataException jde) {
            // Handling stuff like OOM's on Redis' side
            if (purgeOnFailure) {
                errorHandler.error("Can't publish events to Redis at " + getRedisAddress() + ": " + jde.getMessage());
                eventsDroppedInPush += messageIndex;
                batchPurges++;
                messageIndex = 0;
            }
            return false;
        } catch (JedisConnectionException e) {
            LogLog.error("connection error to " + host + ":" + port + " error=" + e.getMessage());
            return false;
        }
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getRedisAddress() {
        return host + ":" + port;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setPurgeOnFailure(boolean purgeOnFailure) {
        this.purgeOnFailure = purgeOnFailure;
    }

    public boolean requiresLayout() {
        return true;
    }

    public int getEventCounter() {
        return eventCounter;
    }

    public int getEventsDroppedInQueueing() {
        return eventsDroppedInQueueing;
    }

    public int getEventsDroppedInPush() {
        return eventsDroppedInPush;
    }

    public int getConnectCounter() {
        return connectCounter;
    }

    public int getConnectFailures() {
        return connectFailures;
    }

    public int getBatchPurges() {
        return batchPurges;
    }

    public int getEventsPushed() {
        return eventsPushed;
    }
}
