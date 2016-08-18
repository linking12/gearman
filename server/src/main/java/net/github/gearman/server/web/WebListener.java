package net.github.gearman.server.web;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.google.inject.servlet.GuiceFilter;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import net.github.gearman.server.config.GearmanServerConfiguration;

public class WebListener {

    private final GearmanServerConfiguration serverConfiguration;

    public WebListener(GearmanServerConfiguration serverConfiguration){
        this.serverConfiguration = serverConfiguration;
    }

    public void start() throws Exception {
        ServletHolder sh = new ServletHolder(ServletContainer.class);
        sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
                            "com.sun.jersey.api.core.PackagesResourceConfig");
        sh.setInitParameter("com.sun.jersey.config.property.packages", "net.github.gearman.server.web");
        sh.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true");

        Server server = new Server(8080);
        ServletContextHandler context = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
        context.addEventListener(new GearmanGuiceServletConfig(serverConfiguration));
        context.addFilter(GuiceFilter.class, "/*", null);
        context.addServlet(sh, "/*");
        server.start();
    }
}
