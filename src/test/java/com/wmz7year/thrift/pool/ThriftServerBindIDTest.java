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

import com.sk.transport.TTransportProvider;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.wmz7year.thrift.pool.config.ThriftConnectionPoolConfig;
import com.wmz7year.thrift.pool.config.ThriftServerInfo;
import com.wmz7year.thrift.pool.connection.ThriftConnection;
import com.wmz7year.thrift.pool.config.ThriftConnectionPoolConfig.TProtocolType;
import com.wmz7year.thrift.pool.config.ThriftConnectionPoolConfig.ThriftServiceType;
import com.wmz7year.thrift.pool.example.Example;
import com.wmz7year.thrift.pool.example.Example.Client;
import com.wmz7year.thrift.pool.example.Other;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/*
 * thrift服务器添加可选的ID表识别  可以根据ID获取对应的服务器
 */
public class ThriftServerBindIDTest extends BasicAbstractTest {

    private List<ThriftServerInfo> servers;

    /*
	 * @see com.wmz7year.thrift.pool.BasicAbstractTest#beforeTest()
     */
    @Override
    protected void beforeTest() throws Exception {
        this.servers = startServers(2);
    }

    /*
	 * @see com.wmz7year.thrift.pool.BasicAbstractTest#afterTest()
     */
    @Override
    protected void afterTest() throws Exception {
        // ignore
    }

    public void testThriftServerBindID() throws Exception {
        ThriftConnectionPoolConfig config = new ThriftConnectionPoolConfig(ThriftServiceType.MULTIPLEXED_INTERFACE);
        config.setConnectTimeout(3000);
        config.setThriftProtocol(TProtocolType.BINARY);
        config.setTransportProvider(new TTransportProvider() {
            @Override
            public TTransport get(String host, int port, int connectionTimeout) throws Exception {
                return new TSocket(host, port, connectionTimeout);
            }
        });
        // 该端口不存在
        ThriftServerInfo thriftServerInfo1 = servers.get(0);
        byte[] nodeID1 = String.format("%s%d", thriftServerInfo1.getHost(), thriftServerInfo1.getPort()).getBytes();
        config.addThriftServer(thriftServerInfo1.getHost(), thriftServerInfo1.getPort(), nodeID1);

        ThriftServerInfo thriftServerInfo2 = servers.get(1);
        byte[] nodeID2 = String.format("%s%d", thriftServerInfo2.getHost(), thriftServerInfo2.getPort()).getBytes();
        config.addThriftServer(thriftServerInfo2.getHost(), thriftServerInfo2.getPort(), nodeID2);

        config.addThriftClientClass("example", Example.Client.class);
        config.addThriftClientClass("other", Other.Client.class);

        config.setMaxConnectionPerServer(2);
        config.setMinConnectionPerServer(1);
        config.setIdleMaxAge(2, TimeUnit.SECONDS);
        config.setMaxConnectionAge(2);
        config.setLazyInit(false);
        config.setAcquireIncrement(2);
        config.setAcquireRetryDelay(2000);

        config.setAcquireRetryAttempts(1);
        config.setMaxConnectionCreateFailedCount(1);
        config.setConnectionTimeoutInMs(5000);

        ThriftConnectionPool<Example.Client> pool = new ThriftConnectionPool<Example.Client>(config);
        ThriftConnection<Client> connection = pool.getConnection(nodeID1);
        assertNotNull(connection);
        connection.close();
        connection = pool.getConnection(nodeID2);
        assertNotNull(connection);
        connection.close();
        pool.close();
    }
}
