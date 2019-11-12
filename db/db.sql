CREATE DATABASE if NOT EXISTS `tesseract` default character set utf8 collate utf8_general_ci;
use `tesseract`;

create table tesseract_job_detail
(
    id          int unsigned primary key auto_increment COMMENT '主键，自增',
    trigger_id  int unsigned not null COMMENT 'tesseract_trigger id',
    class_name  varchar(255) not null COMMENT '要执行的任务的类名',
    create_time bigint       not null COMMENT '创建时间戳',
    creator     varchar(255) not null COMMENT '创建者',
    unique (trigger_id, class_name)
) engine = InnoDB
  default charset = utf8;

create table tesseract_trigger
(
    id                int unsigned primary key auto_increment COMMENT '主键，自增',
    name              varchar(30)  not null COMMENT '触发器名字',
    next_trigger_time bigint       not null COMMENT '下一次触发时间戳',
    prev_trigger_time bigint       not null COMMENT '上一次触发时间戳',
    cron              varchar(30)  not null COMMENT 'cron表达式',
    strategy          tinyint      not null COMMENT '调度策略。0:hash离散,1:轮训,2:均衡负载,3:广播式',
    sharding_num      tinyint      not null COMMENT '分片数量',
    retry_count       tinyint      not null COMMENT '重试次数',
    status            tinyint      not null COMMENT '状态，0停用/1启用',
    creator           varchar(255) not null COMMENT '创建者',
    description       text         not null COMMENT '描述',
    executor_id       int unsigned not null COMMENT '关联执行器id',
    executor_name     varchar(30)  not null COMMENT '冗余执行器名称',
    create_time       bigint       not null COMMENT '创建时间',
    update_time       bigint       not null COMMENT '更新时间',
    group_name        varchar(30)  not null COMMENT '组名称',
    group_id          int unsigned not null COMMENT '关联组',
    params            varchar(255) COMMENT '参数',
    index (status),
    unique (name)
) engine = InnoDB
  default charset = utf8;

create table tesseract_fired_job
(
    id                 int unsigned primary key auto_increment COMMENT '主键，自增',
    trigger_id         int unsigned not null COMMENT '关联触发器id',
    trigger_name       varchar(255) not null COMMENT '触发器名',
    class_name         varchar(255) not null COMMENT '执行类名',
    job_id             int unsigned not null COMMENT '任务id',
    executor_detail_id int unsigned not null COMMENT '关联具体执行机器id',
    socket             varchar(255) not null COMMENT '执行机器：ip:端口',
    create_time        bigint       not null COMMENT '创建时间',
    log_id             int unsigned not null COMMENT '关联日志id',
    retry_count        int unsigned not null COMMENT '重试次数',
    sharding_index     tinyint      not null COMMENT '分片索引',
    group_id           int unsigned not null COMMENT '关联组',
    group_name         varchar(30)  not null COMMENT '组名称',
    creator            varchar(255) not null COMMENT '创建者'
) engine = InnoDB
  default charset = utf8;

create table tesseract_executor
(
    id          int unsigned primary key auto_increment COMMENT '主键，自增',
    name        varchar(30)  not null COMMENT '执行器名称',
    creator     varchar(255) not null COMMENT '创建者',
    description text         not null COMMENT '描述',
    create_time bigint       not null COMMENT '创建时间',
    group_name  varchar(30)  not null COMMENT '所属组名称',
    group_id    int unsigned not null COMMENT '所属组id',
    mail        varchar(255) not null COMMENT '冗余邮箱',
    unique (name)
) engine = InnoDB
  default charset = utf8;

create table tesseract_executor_detail
(
    id          int unsigned primary key auto_increment COMMENT '主键，自增',
    executor_id int unsigned  not null COMMENT '关联执行器',
    load_factor double(10, 2) not null COMMENT '负载权限',
    socket      varchar(255)  not null COMMENT 'ip:端口',
    create_time bigint        not null COMMENT '创建时间',
    update_time bigint        not null COMMENT '更新时间',
    group_name  varchar(30)   not null COMMENT '组名称',
    group_id    int unsigned  not null COMMENT '关联组id',
    unique (socket)
) engine = InnoDB
  default charset = utf8;



create table tesseract_lock
(
    id         int unsigned primary key auto_increment COMMENT '主键，自增',
    group_name varchar(30) not null COMMENT '组名称',
    name       varchar(30) not null COMMENT '名称',
    unique (group_name, name)
) engine = InnoDB
  default charset = utf8;

create table tesseract_user
(
    id          int unsigned primary key auto_increment COMMENT '主键，自增',
    name        varchar(30)  not null COMMENT '用户名称',
    password    varchar(255) not null COMMENT '用户密码加密',
    status      tinyint      not null COMMENT '用户状态',
    create_time bigint       not null COMMENT '创建时间',
    update_time bigint       not null COMMENT '更新时间',
    group_name  varchar(30)  not null COMMENT '关联组名称',
    group_id    int unsigned not null COMMENT '关联组id',
    unique (name)
) engine = InnoDB
  default charset = utf8;

create table tesseract_token
(
    id          int unsigned primary key auto_increment,
    user_id     int unsigned not null,
    user_name   varchar(30)  not null,
    token       varchar(255) not null default '',
    create_time bigint       not null,
    expire_time bigint       not null,
    update_time bigint       not null,
    unique (user_id)
) engine = InnoDB
  default charset = utf8;

create table tesseract_log
(
    id                 bigint unsigned primary key auto_increment COMMENT '主键，自增',
    trigger_name       varchar(30)  not null COMMENT '触发器名称',
    class_name         varchar(255) not null COMMENT 'job类名',
    group_name         varchar(30)  not null COMMENT '组名称',
    group_id           int unsigned not null COMMENT '关联组id',
    socket             varchar(255) not null COMMENT 'ip:端口',
    status             tinyint      not null COMMENT '状态 0=失败；1=成功；2=等待；3=未确认',
    msg                text         not null COMMENT '状态信息',
    creator            varchar(255) not null COMMENT '创建者',
    create_time        bigint       not null COMMENT '创建时间',
    end_time           bigint       not null COMMENT '结束时间',
    executor_detail_id int          not null COMMENT '关联执行器',
    strategy           varchar(30)  not null,
    sharding_index     tinyint      not null,
    retry_count        int unsigned not null COMMENT '重试次数',
    index (create_time),
    index (executor_detail_id)
) engine = InnoDB
  default charset = utf8;


create table tesseract_group
(
    id              int unsigned primary key auto_increment COMMENT '主键，自增',
    name            varchar(30)  not null COMMENT '名称',
    mail            varchar(255) not null COMMENT '邮箱',
    thread_pool_num int          not null COMMENT '线程池大小',
    description     varchar(255) not null default '' COMMENT '描述',
    creator         varchar(255) not null COMMENT '创建者',
    create_time     bigint       not null COMMENT '创建时间',
    update_time     bigint       not null COMMENT '更新时间'
) engine = InnoDB
  default charset = utf8;



insert into tesseract_group(id, name, mail, thread_pool_num, description, creator, create_time, update_time)
values (1, '默认调度组', '', 0, '默认调度将不会发送任何邮件', 'admin', 1562512500000, 1562512500000),
       (2, 'dev1', '', 10, 'liangxuekai@koolearn-inc.com', 'admin', 1562512500000, 1562512500000);
insert into tesseract_user(id, name, password, status, create_time, update_time, group_name, group_id)
values (1, 'super_admin', '$2a$10$uVpmOfuXvWt7bKsD9VQJa.fSfuuLAt94a/e1WNlJ691aJ7rTWfni.', 1, 1562336661000,
        1562336661000,
        'defaultGroup', 1);
insert into tesseract_trigger( name, next_trigger_time, prev_trigger_time, cron, strategy, sharding_num, retry_count
                             , status, creator, description, executor_id, executor_name, create_time, update_time
                             , group_id, group_name)
values ( 'testTrigger-1', 1562512500000, 0, '*/15 * * * * ?', 0, 0, 0, 0, 'admin', 'test', 1, 'testExecutor'
       , 1562512500000, 1562512500000, 2, 'dev1');
insert into tesseract_executor(id, name, creator, description, create_time, group_name, group_id, mail)
values (1, 'testExecutor', 'admin', 'test', 1562512500000, 'defaultGroup', 2, '');



truncate table tesseract_job_detail;
truncate table tesseract_log;
truncate table tesseract_trigger;

drop procedure insert_trigger;
DELIMITER //
CREATE PROCEDURE insert_trigger(IN loop_times INT)
BEGIN
    DECLARE var INT DEFAULT 1;
    start transaction ;
    WHILE var <= loop_times DO
    insert into tesseract_trigger( id, name, next_trigger_time, prev_trigger_time, cron, strategy, sharding_num
                                 , retry_count, status, creator, description, executor_id, executor_name, create_time
                                 , update_time, group_name, group_id)
    values (var, concat('testTrigger-', var), 1562512500000, 0, '*/5 * * * * ?', 0, 0, 0, 1, 'admin', 'test', 1,
            'testExecutor', 1562512500000, 1562512500000, 'dev1', 2);
    insert into tesseract_job_detail(id, trigger_id, class_name, create_time, creator)
    values (var, var, 'tesseract.sample.TestJob', 1562512500000, 'test');
    SET var = var + 1;
    END WHILE;
    commit;
END
//
DELIMITER ;
call insert_trigger(2000);