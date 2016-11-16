/**
 *  				Copyright 2015 Jiang Wei
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.wmz7year.thrift.pool;

import com.netflix.astyanax.retry.RetryNTimes;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.RunOnceRetryPolicyFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TServiceClient;

import com.wmz7year.thrift.pool.config.ThriftServerInfo;
import com.wmz7year.thrift.pool.connection.ThriftConnection;
import com.wmz7year.thrift.pool.exception.ThriftConnectionPoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认连接池获取策略实现类
 *
 * @author jiangwei (ydswcy513@gmail.com)
 * @version V1.0
 */
public class DefaultThriftConnectionStrategy<T extends TServiceClient> extends AbstractThriftConnectionStrategy<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultThriftConnectionStrategy.class);
    private static final long serialVersionUID = 142121086900189271L;

    public DefaultThriftConnectionStrategy(ThriftConnectionPool<T> pool) {
        this.pool = pool;
    }

    /*
	 * @see com.wmz7year.thrift.pool.AbstractThriftConnectionStrategy#
	 * getConnectionInternal()
     */
    @Override
    protected ThriftConnection<T> getConnectionInternal() throws ThriftConnectionPoolException {
        ThriftConnection<T> result = pollConnection();
        // 如果立即获取连接失败 则换一个分区继续获取
        // TODO 设置当连接获取超时返回null？
        if (result == null) {
            if (this.pool.getThriftServerCount() == 0) {
                throw new ThriftConnectionPoolException("Thrift servers count is 0.");
            }
            int partition = (int) (Thread.currentThread().getId() % this.pool.thriftServerCount);

            ThriftConnectionPartition<T> thriftConnectionPartition = this.pool.partitions.get(partition);

            try {
                result = thriftConnectionPartition.poolFreeConnection(this.pool.connectionTimeoutInMs,
                        TimeUnit.MILLISECONDS);
                if (result == null) {
                    throw new ThriftConnectionPoolException("Timed out waiting for a free available connection.");
                } else if (result.isClosed()) {

                    throw new ThriftConnectionPoolException("Pooled a closed connection.");
                }
            } catch (InterruptedException e) {
                throw new ThriftConnectionPoolException(e);
            }
        }
        return result;
    }

    /*
	 * @see com.wmz7year.thrift.pool.AbstractThriftConnectionStrategy#
	 * getConnectionInternal(byte[])
     */
    @Override
    protected ThriftConnection<T> getConnectionInternal(byte[] nodeID) throws ThriftConnectionPoolException {
        if (nodeID == null) {
            throw new NullPointerException();
        }
        if (this.pool.getThriftServerCount() == 0) {
            throw new ThriftConnectionPoolException("当前没有可用的服务器  无法获取连接");
        }

        List<ThriftConnectionPartition<T>> partitions = Collections.unmodifiableList(this.pool.partitions);
        ThriftConnectionPartition<T> thriftConnectionPartition = null;
        for (ThriftConnectionPartition<T> tempPartition : partitions) {
            ThriftServerInfo thriftServerInfo = tempPartition.getThriftServerInfo();
            if (Arrays.equals(thriftServerInfo.getServerID(), nodeID)) {
                thriftConnectionPartition = tempPartition;
                break;
            }
        }
        if (thriftConnectionPartition == null) {
            throw new ThriftConnectionPoolException("没有找到对应服务器节点：" + Arrays.toString(nodeID));
        }
        RetryPolicy pollingConnectionRetryPolicy = this.pool.getConfig().getPollingConnectionRetryPolicy();
        ThriftConnection<T> result = null;
        pollingConnectionRetryPolicy.begin();

        do {
            try {
                result = thriftConnectionPartition.poolFreeConnection(this.pool.connectionTimeoutInMs,
                        TimeUnit.MILLISECONDS);
                if (result == null) {

                } else if (result.isClosed()) {
                    pool.releaseConnection(result);
                    thriftConnectionPartition.freePartitions();
                } else {
                    pollingConnectionRetryPolicy.success();
                }
            } catch (InterruptedException ex) {
                LOGGER.error("Fail to poll free connection from pool. Attemp count: {}. ex = {}",
                        new Object[]{pollingConnectionRetryPolicy.getAttemptCount(), ex.getLocalizedMessage(), ex});
            }
        } while (pollingConnectionRetryPolicy.allowRetry());

        // After retry polling connection.
        try {
            if (result == null) {
                return createConnection(thriftConnectionPartition);
            } else {
                return result;
            }
        } catch (ThriftConnectionPoolException ex) {
            terminateAllConnections();
            ThriftConnectionPoolException e = new ThriftConnectionPoolException("Cannot obtain connection. The connection to server may be broken.");
            e.addSuppressed(ex);
            throw e;
        }

    }

    /*
	 * @see
	 * com.wmz7year.thrift.pool.ThriftConnectionStrategy#terminateAllConnections
	 * ()
     */
    @Override
    public void terminateAllConnections() {
        this.terminationLock.lock();
        try {
            for (int i = 0; i < this.pool.thriftServerCount; i++) {
                this.pool.partitions.get(i).setUnableToCreateMoreTransactions(false);
                List<ThriftConnectionHandle<T>> clist = new LinkedList<>();
                this.pool.partitions.get(i).getFreeConnections().drainTo(clist);
                for (ThriftConnectionHandle<T> c : clist) {
                    this.pool.destroyConnection(c);
                }

            }
        } finally {
            this.terminationLock.unlock();
        }
    }

    /*
	 * @see com.wmz7year.thrift.pool.ThriftConnectionStrategy#pollConnection()
     */
    @Override
    public ThriftConnection<T> pollConnection() {
        ThriftConnection<T> result;
        if (pool.getThriftServerCount() == 0) {
            throw new IllegalStateException("当前无可用连接服务器");
        }
        int partition = (int) (Thread.currentThread().getId() % this.pool.thriftServerCount);

        ThriftConnectionPartition<T> thriftConnectionPartition = this.pool.partitions.get(partition);

        result = thriftConnectionPartition.poolFreeConnection();
        if (result == null) {
            for (int i = 0; i < this.pool.thriftServerCount; i++) {
                if (i == partition) {
                    continue;
                }
                result = this.pool.partitions.get(i).poolFreeConnection();
                if (result != null) {
                    thriftConnectionPartition = this.pool.partitions.get(i);
                    break;
                }
            }
        }

        if (!thriftConnectionPartition.isUnableToCreateMoreTransactions()) {
            this.pool.maybeSignalForMoreConnections(thriftConnectionPartition);
        }
        return result;
    }

    public ThriftConnectionHandle<T> createConnection(ThriftConnectionPartition<T> thriftConnectionPartition) throws ThriftConnectionPoolException {
        return new ThriftConnectionHandle<>(null, thriftConnectionPartition, pool, false);
    }

}
