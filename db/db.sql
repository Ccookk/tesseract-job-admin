CREATE DATABASE if NOT EXISTS `tesseract` default character set utf8 collate utf8_general_ci;
use `tesseract`;

create table tesseract_job_detail
(
    id          int unsigned primary key auto_increment,
    trigger_id  int unsigned not null,
    class_name  varchar(255) not null,
    create_time bigint       not null,
    creator     varchar(255) not null,
    unique (trigger_id, class_name)
) engine = InnoDB
  default charset = utf8;

create table tesseract_trigger
(
    id                int unsigned primary key auto_increment,
    name              varchar(30)  not null,
    next_trigger_time bigint       not null,
    prev_trigger_time bigint       not null,
    cron              varchar(30)  not null,
    strategy          tinyint      not null,
    sharding_num      tinyint      not null,
    retry_count       tinyint      not null,
    status            tinyint      not null,
    creator           varchar(255) not null,
    description       text         not null,
    executor_id       int unsigned not null,
    executor_name     varchar(30)  not null,
    create_time       bigint       not null,
    update_time       bigint       not null,
    group_name        varchar(30)  not null,
    group_id          int unsigned not null,
    index (status),
    unique (name)
) engine = InnoDB
  default charset = utf8;

create table tesseract_fired_trigger
(
    id                 int unsigned primary key auto_increment,
    triggerId          int unsigned not null,
    class_name         varchar(255) not null,
    name               varchar(30)  not null,
    socket             varchar(255) not null,
    executor_detail_id int unsigned not null,
    create_time        bigint       not null,
    log_id             int unsigned not null
) engine = InnoDB
  default charset = utf8;

create table tesseract_executor
(
    id          int unsigned primary key auto_increment,
    name        varchar(30)  not null,
    creator     varchar(255) not null,
    description text         not null,
    create_time bigint       not null,
    group_name  varchar(30)  not null,
    group_id    int unsigned not null,
    unique (name)
) engine = InnoDB
  default charset = utf8;

create table tesseract_executor_detail
(
    id          int unsigned primary key auto_increment,
    executor_id int unsigned not null,
    load_factor      double(4,2) not null,
    socket      varchar(255) not null,
    create_time bigint       not null,
    update_time bigint       not null,
    unique (socket)
) engine = InnoDB
  default charset = utf8;



create table tesseract_lock
(
    id         int unsigned primary key auto_increment,
    group_name varchar(30) not null,
    name       varchar(30) not null,
    index (group_name, name)
) engine = InnoDB
  default charset = utf8;

create table tesseract_user
(
    id          int unsigned primary key auto_increment,
    name        varchar(30)  not null,
    password    varchar(32)  not null,
    status      tinyint      not null,
    create_time bigint       not null,
    update_time bigint       not null,
    group_name  varchar(30)  not null,
    group_id    int unsigned not null
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
    id           bigint unsigned primary key auto_increment,
    trigger_name varchar(30)  not null,
    class_name   varchar(255) not null,
    group_name   varchar(30)  not null,
    group_id     int unsigned not null,
    socket       varchar(255) not null,
    status       tinyint      not null,
    msg          text         not null,
    creator      varchar(255) not null,
    create_time  bigint       not null,
    end_time     bigint       not null,
    index (create_time)
) engine = InnoDB
  default charset = utf8;


create table tesseract_group
(
    id              int unsigned primary key auto_increment,
    name            varchar(30)  not null,
    mail            varchar(255) not null,
    thread_pool_num int          not null,
    description     varchar(255) not null default '',
    creator         varchar(255) not null,
    create_time     bigint       not null,
    update_time     bigint       not null
) engine = InnoDB
  default charset = utf8;



insert into tesseract_group(id, name, mail, thread_pool_num, creator, create_time, update_time)
values (1, 'defaultGroup', 'xxxx@xx.mail', 10, 'admin', 1562512500000, 1562512500000);
insert into tesseract_user(name, password, status, create_time, update_time, group_id, group_name)
values ('admin', '$2a$10$uVpmOfuXvWt7bKsD9VQJa.fSfuuLAt94a/e1WNlJ691aJ7rTWfni.', 0, 1562336661000, 1562336661000, 1, 'defaultGroup');
insert into tesseract_trigger( name, next_trigger_time, prev_trigger_time, cron, strategy, sharding_num, retry_count
                             , status, creator, description, executor_id, executor_name, create_time, update_time
                             , group_id, group_name)
values ( 'testTrigger', 1562512500000, 0, '*/5 * * * * ?', 0, 0, 0, 0, 'admin', 'test', 1, 'testExecutor'
       , 1562512500000, 1562512500000, 1, 'defaultGroup');
insert into tesseract_executor(name, creator, description, create_time, group_id, group_name)
values ('testExecutor', 'admin', 'test', 1562512500000, 1, 'defaultGroup');








-- truncate table tesseract_job_detail;
-- truncate table tesseract_log;
-- truncate table tesseract_trigger;
-- truncate table tesseract_fired_trigger;
-- drop procedure insert_trigger;
-- DELIMITER //
-- CREATE PROCEDURE insert_trigger(IN loop_times INT)
-- BEGIN
--     DECLARE var INT DEFAULT 1;
--     start transaction ;
--     WHILE var <= loop_times DO
--     insert into tesseract_trigger(id, name, next_trigger_time, prev_trigger_time, cron,
--                                   strategy, sharding_num, retry_count, status, creator, description, executor_id,
--                                   executor_name, create_time, update_time)
--     values (var, concat('testTrigger-', var), 1562512500000, 0, '0 0/5 * * * ?', 0, 0, 0, 1, 'admin', 'test', 1,
--             'testExecutor',
--             1562512500000, 1562512500000);
--
--     insert into tesseract_job_detail(trigger_id, class_name, create_time, creator)
--     values (var, 'tesseract.sample.TestJob', 1562512500000, 'admin');
--     SET var = var + 1;
--     END WHILE;
--     commit;
-- END
-- //
-- DELIMITER ;
-- call insert_trigger(50000);