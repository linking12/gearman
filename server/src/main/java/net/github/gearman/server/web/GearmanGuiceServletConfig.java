package net.github.gearman.server.web;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import net.github.gearman.server.config.GearmanServerConfiguration;
import net.github.gearman.server.config.ServerConfiguration;

public class GearmanGuiceServletConfig extends GuiceServletContextListener {

    private final GearmanServerConfiguration serverConfiguration;

    public GearmanGuiceServletConfig(GearmanServerConfiguration serverConfiguration){
        this.serverConfiguration = serverConfiguration;
    }

    @Override
    protected Injector getInjector() {
        return Guice.createInjector(new JerseyServletModule() {

            @Override
            protected void configureServlets() {
                bind(RESTFulResource.class);
                bind(ServerConfiguration.class).toInstance(serverConfiguration);
                serve("/*").with(GuiceContainer.class);
            }
        });
    }

}
