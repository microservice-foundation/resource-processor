package com.epam.training.microservicefoundation.resourceprocessor;

import com.epam.training.microservicefoundation.resourceprocessor.client.ResourceServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.client.SongServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.Server;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.HashMap;
import java.util.Map;

public class ExternalServerExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {
    private MockWebServer resourceServiceServer;
    private MockWebServer songServiceServer;
    private Map<Class<?>, MockWebServer> servers;
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        resourceServiceServer.close();
        songServiceServer.close();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        resourceServiceServer = new MockWebServer();
        songServiceServer = new MockWebServer();
        servers = new HashMap<>();
        servers.put(ResourceServiceClient.class, resourceServiceServer);
        servers.put(SongServiceClient.class, songServiceServer);

        System.setProperty("resource-service.endpoint", resourceServiceServer.url("/api/v1").url().toString());
        System.setProperty("song-service.endpoint", songServiceServer.url("/api/v1").url().toString());
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == MockWebServer.class &&
                parameterContext.getParameter().isAnnotationPresent(Server.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Server server = parameterContext.getParameter().getDeclaredAnnotation(Server.class);
        return servers.get(server.serveTo());
    }
}
