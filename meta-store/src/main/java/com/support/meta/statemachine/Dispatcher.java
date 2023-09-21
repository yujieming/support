package com.support.meta.statemachine;

import java.util.concurrent.CompletableFuture;

public interface Dispatcher<Request, Response> {

    CompletableFuture<Response> dispatch(Request request);

}
