package net.github.gearman.server.web;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;

import net.github.gearman.server.config.ServerConfiguration;

@Path("/javahonkJetty")
public class RESTFulResource {

    private final ServerConfiguration gerverConfiguration;

    @Inject
    public RESTFulResource(final ServerConfiguration gerverConfiguration){
        this.gerverConfiguration = gerverConfiguration;
    }

    @GET
    @Path("/person/{name}")
    @Produces(MediaType.TEXT_HTML)
    public String getHTMLData(@PathParam("name") String name) {
        System.out.println(gerverConfiguration);
        return name;
    }

}
