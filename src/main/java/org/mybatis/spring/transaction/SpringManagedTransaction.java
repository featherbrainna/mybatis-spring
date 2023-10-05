/**
 *    Copyright 2010-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.mybatis.spring.transaction;

import org.apache.ibatis.transaction.Transaction;
import org.mybatis.logging.Logger;
import org.mybatis.logging.LoggerFactory;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.springframework.util.Assert.notNull;

/**
 * {@code SpringManagedTransaction} handles the lifecycle of a JDBC connection.
 * It retrieves a connection from Spring's transaction manager and returns it back to it
 * when it is no longer needed.
 * <p>
 * If Spring's transaction handling is active it will no-op all commit/rollback/close calls
 * assuming that the Spring transaction manager will do the job.
 * <p>
 * If it is not it will behave like {@code JdbcTransaction}.
 *
 * @author Hunter Presnall
 * @author Eduardo Macarron
 */
public class SpringManagedTransaction implements Transaction {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpringManagedTransaction.class);

  /**
   * 与当前数据库连接对象关联的 数据源对象。构造器初始化
   */
  private final DataSource dataSource;

  /**
   * 当前事务管理中维护的 数据库连接对象
   */
  private Connection connection;

  /**
   * 标志该数据库连接对象是否由 Spring 的事务管理器管理
   */
  private boolean isConnectionTransactional;

  /**
   * 事务是否自动提交
   */
  private boolean autoCommit;

  /**
   * 构造器，初始化 dataSource 属性
   * @param dataSource 数据源对象
   */
  public SpringManagedTransaction(DataSource dataSource) {
    //判断dataSource非空
    notNull(dataSource, "No DataSource specified");
    //初始化属性
    this.dataSource = dataSource;
  }

  /**
   * 获取数据库连接
   * {@inheritDoc}
   */
  @Override
  public Connection getConnection() throws SQLException {
    //1.如果 connection 属性为空，则获取数据库连接并初始化属性
    if (this.connection == null) {
      openConnection();
    }
    //2.返回数据库连接属性
    return this.connection;
  }

  /**
   * 获取数据库连接，并初始化属性 connection、autoCommit、isConnectionTransactional
   * Gets a connection from Spring transaction manager and discovers if this
   * {@code Transaction} should manage connection or let it to Spring.
   * <p>
   * It also reads autocommit setting because when using Spring Transaction MyBatis
   * thinks that autocommit is always false and will always call commit/rollback
   * so we need to no-op that calls.
   */
  private void openConnection() throws SQLException {
    //1.从 Spring 事务管理器中获取数据库连接对象，并设置属性 connection
    this.connection = DataSourceUtils.getConnection(this.dataSource);
    //2.从数据库连接获取事务是否自动提交。
    // 当使用 Spring 来管理事务时，并不会由 SpringManagedTransaction 的commit()和rollback()两个方法来管理事务
    this.autoCommit = this.connection.getAutoCommit();
    //3.设置 isConnectionTransactional 属性
    this.isConnectionTransactional = DataSourceUtils.isConnectionTransactional(this.connection, this.dataSource);

    LOGGER.debug(() ->
        "JDBC Connection ["
            + this.connection
            + "] will"
            + (this.isConnectionTransactional ? " " : " not ")
            + "be managed by Spring");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void commit() throws SQLException {
    //当数据库连接非空，且数据库连接对象不由 Spring 的事务管理器管理，且不自动提交事务
    if (this.connection != null && !this.isConnectionTransactional && !this.autoCommit) {
      LOGGER.debug(() -> "Committing JDBC Connection [" + this.connection + "]");
      this.connection.commit();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rollback() throws SQLException {
    if (this.connection != null && !this.isConnectionTransactional && !this.autoCommit) {
      LOGGER.debug(() -> "Rolling back JDBC Connection [" + this.connection + "]");
      this.connection.rollback();
    }
  }

  /**
   * 关闭数据库连接
   * {@inheritDoc}
   */
  @Override
  public void close() {
    //将数据库连接归还给 spring 事务管理器
    DataSourceUtils.releaseConnection(this.connection, this.dataSource);
  }
    
  /**
   * {@inheritDoc}
   */
  @Override
  public Integer getTimeout() {
    ConnectionHolder holder = (ConnectionHolder) TransactionSynchronizationManager.getResource(dataSource);
    if (holder != null && holder.hasTimeout()) {
      return holder.getTimeToLiveInSeconds();
    } 
    return null;
  }

}
