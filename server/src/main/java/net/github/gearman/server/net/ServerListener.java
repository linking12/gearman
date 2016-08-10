package net.github.gearman.server.net;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.core.QueuedJob;
import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.queue.JobQueue;
import net.github.gearman.engine.queue.factories.cronjob.CronJobQueueFactory;
import net.github.gearman.engine.queue.factories.job.JobQueueFactory;
import net.github.gearman.server.config.ServerConfiguration;

public class ServerListener {

    private final Logger              LOG = LoggerFactory.getLogger(ServerListener.class);

    private final ServerConfiguration serverConfiguration;
    private final EventLoopGroup      bossGroup, workerGroup;

    public ServerListener(ServerConfiguration serverConfiguration){
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        this.serverConfiguration = serverConfiguration;
    }

    public boolean start() {
        LOG.info("Listening on " + serverConfiguration.getHostName() + ":" + serverConfiguration.getPort());

        LOG.info("Loading existing jobs...");
        // Load up jobs
        JobQueueFactory jobQueueFactory = serverConfiguration.getJobQueueFactory();
        CronJobQueueFactory cronJobQueueFactory = serverConfiguration.getCronJobQueueFactory();
        JobManager jobManager = serverConfiguration.getJobManager();

        if (jobQueueFactory != null) {
            Collection<QueuedJob> queuedJobs = jobQueueFactory.loadPersistedJobs();
            Set<String> queueNames = queuedJobs.parallelStream().map(p -> p.functionName).collect(Collectors.toSet());

            int imported = 0;
            for (String functionName : queueNames) {
                try {
                    JobQueue queue = jobManager.getOrCreateJobQueue(functionName);
                    imported += queue.size();
                } catch (JobQueueFactoryException e) {
                    e.printStackTrace();
                }
            }
            LOG.info("Imported " + imported + " persisted jobs.");
        }
        if (cronJobQueueFactory != null) {
            cronJobQueueFactory.triggerCronJob(jobManager);
        }

        final NetworkManager networkManager = new NetworkManager(serverConfiguration.getJobManager());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)//
                     .channel(NioServerSocketChannel.class)//
                     .childHandler(new GearmanServerInitializer(networkManager, serverConfiguration.isSSLEnabled()))//
                     .option(ChannelOption.SO_KEEPALIVE, true)//
                     .option(ChannelOption.SO_REUSEADDR, true)//
                     .option(ChannelOption.TCP_NODELAY, true);//
            bootstrap.bind(serverConfiguration.getPort()).sync().channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

        return true;
    }

    public void stop() {
        LOG.debug("Gearman server stopping...");
        bossGroup.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
        LOG.info("Gearman server stopped");
    }

}
