# SynchDB 开发环境配置指南

# 1. 概要

要进行 SynchDB 开发，你需要一个 **Linux开发环境**（比如 Ubuntu） 用来编译 SyncDB代码，以及运行 MySQL 、 MS SQL的机器作为源端数据库供 SynchDB 测试使用。 SyncDB 、 MySQL 、和 MS SQL 可以都运行在同一台机器上，也可以分别在不同机器。

SyncDB 的环境设置在项目的 README 中有详细描述：
[https://github.com/Hornetlabs/synchdb/blob/synchdb-devel/README.md](https://github.com/Hornetlabs/synchdb/blob/synchdb-devel/README.md)

> [!CAUTION]
> 目前SynchDB项目仍在初始开发阶段，代码分支为 **synchdb-devel**

# 2. GitHub 账户

在准备开发之前，你需要有一个 GitHub 账户，并 Fork SynchDB 项目到你的个人账户内。

[如何 Fork SynchDB 到你的个人 GithHub 账户？
1. 到 [https://github.com/Hornetlabs/synchdb](https://github.com/Hornetlabs/synchdb) 页面
2. 点击 Fork，或者直接访问[https://github.com/Hornetlabs/synchdb/fork](https://github.com/Hornetlabs/synchdb/fork)](https://www.notion.so/Fork-SynchDB-GithHub-1-https-github-com-Hornetlabs-synchdb-2-Fork-https--ab548786925649c5bec803cfc0969929?pvs=21)

# 3. Linux 开发环境设置

你需要一台 Linux 开发环境进行 SyncDB 开发，这里以MacOS环境下安装Ubuntu 22.04 LTS 为例。

本文所有的虚拟机都是使用轻量化的 Multipass 工具来创建的，你完全可以使用其他虚拟化工具比如Vmware的Workstation 、 VirtualBox等。

## 3.1 安装 Multipass

```bash
brew install multipass
```

Multipass 支持 Linux 、 Mac 、 Windows 安装，访问官网查看如何安装与使用：

安装指南：[https://multipass.run/docs/install-multipass#install](https://multipass.run/docs/install-multipass#install)

使用指南：[https://multipass.run/docs/manage-instances](https://multipass.run/docs/manage-instances)

参考文章： [https://juejin.cn/post/7189945206085517367](https://juejin.cn/post/7189945206085517367)

## 3.2 安装Ubuntu 22.04 LTS 虚拟机

运行下面的命令安装并启动一台 4 个 CPU，8G 内存，100G 硬盘的 Ubuntu 22.04，命名为 synchdb：

```bash
multipass launch 22.04 -n synchdb -c 4 -m 8G -d 100G
```

命令输出举例：

```bash
grantzhou@synchdb:~$ **multipass --version**
multipass   1.14.0+mac
multipassd  1.14.0+mac
grantzhou@synchdb:~$ **multipass launch 22.04 -n synchdb -c 2 -m 8G -d 50G**
Launched: synchdb

grantzhou@synchdb:~$ **multipass mount ~/vmshared synchdb:/home/ubuntu/vmshared/**

grantzhou@synchdb:~$ **multipass info synchdb**
Name:           synchdb
State:          Running
Snapshots:      0
IPv4:           192.168.64.6
Release:        Ubuntu 22.04.4 LTS
Image hash:     f586a0825761 (Ubuntu 22.04 LTS)
CPU(s):         2
Load:           0.10 0.03 0.01
Disk usage:     1.6GiB out of 48.4GiB
Memory usage:   173.6MiB out of 7.7GiB
Mounts:         /Users/grantzhou/vmshared => /home/ubuntu/vmshared
                    UID map: 501:default
                    GID map: 20:default
```

具体 `multipass` 的使用命令，参考官网手册 [https://multipass.run/docs/launch-command](https://multipass.run/docs/launch-command)

登录到此 Ubuntu 系统：

```bash
multipass shell synchdb
```

## 3.3 设置 Git 环境

Multipass 所安装的 Ubuntu 已经默认安装 `git` 命令，建议采用 SSH 方式连接 GitHub 以方便平时工作，从而无需在每次访问时都提供用户名和 personal access token。 另外还可以使用 SSH 密钥对提交进行签名。

新建一个 SSH 密钥，将示例中使用的电子邮件替换为 GitHub 电子邮件地址。当提示“Enter a file in which to save the key（输入要保存密钥的文件）”时以及提示你输入密码时，直接回车即可。

```bash
ssh-keygen -t ed25519 -C "your_email@example.com"
```

输入 `ls -al ~/.ssh` 查看现有的 SSH 密钥：

```bash
ubuntu@synchdb:~$ ls -al ~/.ssh
total 20
drwx------ 2 ubuntu ubuntu 4096 Aug  6 12:33 .
drwxr-x--- 6 ubuntu ubuntu 4096 Aug  6 12:20 ..
-rw------- 1 ubuntu ubuntu  398 Aug  6 12:04 authorized_keys
-rw------- 1 ubuntu ubuntu  411 Aug  6 12:33 id_ed25519
-rw-r--r-- 1 ubuntu ubuntu  101 Aug  6 12:33 id_ed25519.pub
```

将 SSH 公钥添加到 GitHub 上的帐户，复制 `~/.ssh/id_ed25519.pub` 文件内容

```
$ cat ~/.ssh/id_ed25519.pub
# Then select and copy the contents of the id_ed25519.pub file
# displayed in the terminal to your clipboard
```

1. 在 GitHub 任意页的右上角，单击个人资料照片，然后单击“**设置**”
2. 在边栏的“访问”部分中，单击 “SSH 和 GPG 密钥”
3. 单击“新建 SSH 密钥”。
4. 在 "Title"（标题）字段中，为新密钥添加描述性标签。 例如 `synchdb-ssh-key`
5. 选择密钥类型（身份验证或签名）
6. 在“密钥”字段中，粘贴公钥
7. 单击“添加 SSH 密钥”

## 3.4 编译并安装 PostgreSQL 16.3

1. 下载 Postgres 代码，并切换到 16.3 分支

```bash
git clone https://github.com/postgres/postgres.git
cd postgres && git checkout REL_16_3 -b pg163
```

2. 编译 PostgreSQL 代码

    a. 准备安装环境

    ```bash
    sudo apt-get install -y build-essential gdb lcov bison flex \
                 libkrb5-dev libssl-dev libldap-dev libpam0g-dev \
                 python3-dev tcl-dev libperl-dev gettext libxml2-dev \
                 libxslt1-dev libreadline-dev libedit-dev uuid-dev \
                 libossp-uuid-dev libipc-run-perl perl libtest-simple-perl \
                 pkg-config
    ```

    b. 编译&安装 Postgres

    ```bash
    ./configure  --prefix=**/home/ubuntu/pg163/pgapp** --enable-tap-tests --enable-debug CFLAGS="-g3 -O0"

    make –j
    make install
    ```

    c. 运行 Postgres

    ```bash
    export PGDATA=/home/ubuntu/synchdbtest
    export PATH=/home/ubuntu/pg163/pgapp/bin:$PATH
    export LD_LIBRARY_PATH=/home/ubuntu/pg163/pgapp/lib
    initdb -D $PGDATA
    pg_ctl -D $PGDATA -l logfile start

    psql -d postgres 	
    ```

    ```bash
    # Stop Postgres Service
    pg_ctl -D $PGDATA stop
    ```


# 4. SynchDB 开发环境设置

## 4.1 安装 JDK, Maven 及 Docker 环境

```bash
sudo apt install openjdk-21-jdk
sudo apt install maven
sudo snap install docker
```

## 4.2 下载 SynchDB 并编译安装

> [!IMPORTANT]
> 注意：请务必 Fork SynchDB 到个人 GitHub 账户

> [!TIP]
> 如何 Fork SynchDB 到你的个人 GithHub 账户？
> 1. 到 [https://github.com/Hornetlabs/synchdb](https://github.com/Hornetlabs/synchdb) 页面
> 2. 点击 Fork，或者直接访问[https://github.com/Hornetlabs/synchdb/fork](https://github.com/Hornetlabs/synchdb/fork)

下面是下载并编译 SynchDB 的步骤：

1. 到 Postgres 代码目录，下载代码

    ```bash
    cd contrib/
    git clone git@github.com:grantzhou/synchdb.git
    # 切换到 synchdb-devel 分支
    git switch -c synchdb-devel remotes/origin/synchdb-devel
    ```

2. 编译 & 安装 **Debezium Engine**

    ```bash
    cd ~/postgres/contrib/synchdb
    make check_jdk
    make build_dbz
    # 安装 Debezium 引擎到 PostgreSQL lib 路径
    sudo make install_dbz
    ```

3. 编译 SynchDB 扩展 & 安装

    ```bash
    cd ~/postgres/contrib/synchdb
    make & make install
    ```

4. SynchDB 运行依赖设置

    ```bash
    # Dynamically set JDK paths
    JAVA_PATH=$(which java)
    JDK_HOME_PATH=$(readlink -f ${JAVA_PATH} | sed 's:/bin/java::')
    JDK_LIB_PATH=${JDK_HOME_PATH}/lib

    echo $JDK_LIB_PATH
    echo $JDK_LIB_PATH/server

    # 将上面两个目录加入到 x86_64-linux-gnu.conf
    sudo echo "$JDK_LIB_PATH" ｜ sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf
    sudo echo "$JDK_LIB_PATH/server" | sudo tee -a /etc/ld.so.conf.d/x86_64-linux-gnu.conf

    # MAC M1/M2 文件为 aarch64-linux-gnu.conf
    # sudo echo "$JDK_LIB_PATH"       ｜ sudo tee -a /etc/ld.so.conf.d/aarch64-linux-gnu.conf
    # sudo echo "$JDK_LIB_PATH/server" | sudo tee -a /etc/ld.so.conf.d/aarch64-linux-gnu.conf

    sudo ldconfig
    ```

5. 使用 `ldd` 验证 SynchDB 可用

    确保其依赖库都可以被找到

    ```bash
    cd ~/postgres/contrib/synchdb
    ldd synchdb.so

    ubuntu@synchdb:~/postgres/contrib/synchdb$ **ldd synchdb.so**
    	linux-vdso.so.1 (0x0000ffff9db46000)
    	**libjvm.so** => /usr/lib/jvm/java-21-openjdk-arm64/lib/server/libjvm.so (0x0000ffff9c6e0000)
    	libc.so.6 => /lib/aarch64-linux-gnu/libc.so.6 (0x0000ffff9c530000)
    	/lib/ld-linux-aarch64.so.1 (0x0000ffff9db0d000)
    	libstdc++.so.6 => /lib/aarch64-linux-gnu/libstdc++.so.6 (0x0000ffff9c300000)
    	libm.so.6 => /lib/aarch64-linux-gnu/libm.so.6 (0x0000ffff9c260000)
    	libgcc_s.so.1 => /lib/aarch64-linux-gnu/libgcc_s.so.1 (0x0000ffff9c230000)
    ```


# 5. 用于测试的 MySQL数据库 及 MS SQL数据库 安装

使用 `synchdb\` 目录下的 **synchdb-mysql-test.yaml** 和 **synchdb-sqlserver-test.yaml** 两个文件创建两个 Docker

> [!TIP]
> 如果你需要使用支持 SSL 的 Ms SQL Server, 使用 **synchdb-sqlserver-withssl-test.yaml**

## 5.1 安装并启动MySQL

```bash
docker compose -f synchdb-mysql-test.yaml up -d
```

## 5.2 测试 MySQL

连接 MySQL 设置复制权限：

```bash
mysql -h 127.0.0.1 -u root -p
GRANT replication client on *.* to mysqluser;
GRANT replication slave  on *.* to mysqluser;
GRANT RELOAD ON *.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;
```

> 用户需要安装 mysql-client 工具用于链接 MySQL 数据库
> 比如：sudo apt install mysql-client-core-8.0

## 5.3 准备并启动 Ms SQL Server

```
docker compose -f synchdb-sqlserver-test.yaml up -d

```

如果你需要使用支持 SSL 的 Ms SQL Server，可以参考下面做法

1）准备认证文件

准备 .key 和 .pem 文件，synchdb项目内已经生成一套认证文件，如果你希望自己创建一套，可以使用下面命令：

```bash
# create your own key
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout cynchdb-private.key -out synchdb-cert.pem -subj "/CN=synchdb"

# merge the pem and key file
cat synchdb-private.key synchdb-cert.pem > synchdb-combined-cert.pem
```

2）启动 SQL Server

```bash
docker compose -f **synchdb-sqlserver-withssl-test.yaml** up -d
```

## 5.4 准备 SQL Server 的测试数据

使用 sqlcmd 命令准备测试数据，你可以连接到运行SQL Server 的 Docker 内进行，也可以在其他机器上进行。

```bash
id=$(docker ps | grep sqlserver | awk '{print $1}')

docker exec -it $id bash

#Build the database according to the schema:
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -i /inventory.sql

# Run some simple queries:
/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "insert into orders(order_date, purchaser, quantity, product_id) values( '2024-01-01', 1003, 2, 107)"

/opt/mssql-tools/bin/sqlcmd -U sa -P $SA_PASSWORD -d testDB -Q "select * from orders"

```

## 6. 如何修改代码

 代码修改采用提交 PR 方式进行，开发人员需要先在本地开发，并提交代码到个人 GitHub 的 synchdb 代码库，提交代码的时候创建临时分支用于在 GitHub 上创建 PR （Pull Request）：

```bash
git status
git checkout synchdb-devel
# modify your code on synchdb-devel branch first
# add the modified files
git add <file Name>
git commit -m "Your commit message here"
git checkout -b your-new-branch-name
git push origin your-new-branch-name
```

代码提交后，到 GitHub 页面，提交 PR 到 synchdb 项目，请注意选择正确的 Branch 名字。

# 6. 测试 SynchDB

使用 psql 连接到 Postgres 数据库，创建 synchdb 扩展，并执行测试：

```sql

CREATE EXTENSION synchdb;
select synchdb_start_engine_bgw('127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', '', 'mysql');
# specify table name
# select synchdb_start_engine_bgw('127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', 'inventory.orders,inventory.customers', 'mysql');

create database sqlserverdb;
select synchdb_start_engine_bgw('127.0.0.1',1433, 'sa', 'Password!', 'testDB', 'sqlserverdb', '', 'sqlserver');

```

[参见 Readme 文件](https://github.com/Hornetlabs/synchdb/blob/synchdb-devel/README.md)
