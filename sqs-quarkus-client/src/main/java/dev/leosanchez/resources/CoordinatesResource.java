package dev.leosanchez.resources;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import dev.leosanchez.services.CoordinatesService;
import io.vertx.core.json.JsonObject;

@Path("/coordinates")
public class CoordinatesResource {
    @Inject
    CoordinatesService coordinatesService;

    // endpoint for two way communication
    @GET
    @Path("/search/{query}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response query(@PathParam("query") String query) {
        
        JsonObject messageReceived = coordinatesService.queryCoordinates(query);
        if (messageReceived != null) {
            return Response.ok(messageReceived).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        
    }

    // endpoint for one way commmunication
    @POST
    @Path("/submit")
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(JsonObject body) {
        try {
            coordinatesService.submitCoordinates(body.getString("name"), body.getDouble("lat"), body.getDouble("lon"));
            return Response.ok().build();
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
    }
}
