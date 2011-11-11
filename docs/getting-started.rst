.. How to start running and using Vumi

Starting a first Vumi instance::

  twistd -n --pidfile=echoworker.pid start_worker --worker-class vumi.demos.words.EchoWorker --set-option=transport_name:telnet &
  twistd -n --pidfile=telnettransport.pid start_worker --worker-class vumi.transports.telnet.TelnetServerTransport --set-option=transport_name:telnet --set-option=telnet_port:9010 &
  telnet localhost 9010

Second a Vumi instance with 2 workers::

  twistd -n --pidfile=echoworker.pid start_worker --worker-class vumi.demos.words.EchoWorker --set-option=transport_name:telnet &
  twistd -n --pidfile=reverseworker.pid start_worker --worker-class vumi.demos.words.ReverseWorker --set-option=transport_name:telnet &  
  twistd -n --pidfile=telnettransport.pid start_worker --worker-class vumi.transports.telnet.TelnetServerTransport --set-option=transport_name:telnet --set-option=telnet_port:9010 &
  telnet localhost 9010
 
  in this case each worker answer alternatively to an incoming message. To have the 2 workers answering each message, you need to add a dispasher worker. 

Third let's have them run with supervisord to run the worker and transporter and monitor their exchange of messages.

  Create a supervisord.myfirstvumi.conf containing

                [unix_http_server]
                file=./tmp/supervisor.sock   ; (the path to the socket file)
                
                [inet_http_server]         ; inet (TCP) server disabled by default
                port=127.0.0.1:9010        ; (ip_address:port specifier, *:port for all iface)
                
                [supervisord]
                logfile=./logs/supervisord.log ; (main log file;default $CWD/supervisord.log)
                logfile_maxbytes=50MB       ; (max main logfile bytes b4 rotation;default 50MB)
                logfile_backups=10          ; (num of main logfile rotation backups;default 10)
                loglevel=debug               ; (log level;default info; others: debug,warn,trace)
                pidfile=./tmp/pids/supervisord.pid ; (supervisord pidfile;default supervisord.pid)
                nodaemon=false              ; (start in foreground if true;default false)
                minfds=1024                 ; (min. avail startup file descriptors;default 1024)
                minprocs=200                ; (min. avail process descriptors;default 200)
                
                [rpcinterface:supervisor]
                supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface
                
                [supervisorctl]
                serverurl=http://127.0.0.1:9020 ; use an http:// url to specify an inet socket
                
                [program:echo_worker]
                numprocs=1
                numprocs_start=1
                process_name=%(program_name)s_%(process_num)s
                command=twistd -n
                    --pidfile=./tmp/pids/%(program_name)s_%(process_num)s.pid
                    start_worker
                    --worker-class=vumi.demos.words.EchoWorker
                    --set-option=transport_name:telnet
                stdout_logfile=./logs/%(program_name)s_%(process_num)s.log
                stdout_logfile_maxbytes=10MB
                stdout_logfile_backups=10
                stderr_logfile=./logs/%(program_name)s_%(process_num)s.err
                stderr_logfile_maxbytes=10MB
                stderr_logfile_backups=10
                autorestart=true
                
                [program:telnet_transport]
                numprocs=1
                numprocs_start=1
                process_name=%(program_name)s_%(process_num)s
                command=twistd -n
                    --pidfile=./tmp/pids/%(program_name)s_%(process_num)s.pid
                    start_worker
                    --worker-class=vumi.transports.telnet.TelnetServerTransport
                    --set-option=transport_name:telnet
                    --set-option=telnet_port:9010
                stdout_logfile=./logs/%(program_name)s_%(process_num)s.log
                stdout_logfile_maxbytes=10MB
                stdout_logfile_backups=10
                stderr_logfile=./logs/%(program_name)s_%(process_num)s.err
                stderr_logfile_maxbytes=10MB
                stderr_logfile_backups=10
                autorestart=true

   Then as before connect to vumi via telnet
   telnet localhost 9010
   
   ==Monitor==
   You can now monitor/start/stop your worker and transporter via the HTTP supervisord UI
   http://localhost:9020/
   
   You can also monitor the messages exchange in RabbitMQ via the rabbitmqctl command line tool. 
   sudo rabbitmqctl -c list_queues /develop
   It shows you all active queue withing a given vhost (in my case "/develop"). You may have vumi configure differently on your machine to use another vhost. In such a case check out the vhost available with "sudo rabbitmqctl list_vhosts".
   
   Now you can play to exprience the process of messages. First stoping your worker via supervisord and continu sending telnet messages then you can see the message count of your queue increasing. By restarting the worker the messages will be process and the queue lenght go back to zero.