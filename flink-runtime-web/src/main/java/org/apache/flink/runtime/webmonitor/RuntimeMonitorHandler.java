/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.RedirectHandler;
import org.apache.flink.runtime.rest.handler.WebHandler;
import org.apache.flink.runtime.rest.handler.legacy.RequestHandler;
import org.apache.flink.runtime.rest.handler.util.HandlerRedirectUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

<<<<<<< HEAD
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
=======
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.KeepAliveWrite;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.rest.handler.util.HandlerRedirectUtils.ENCODING;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Netty channel handler that processes all HTTP requests.
 * This handler takes the path parameters and delegates the work to a {@link RequestHandler}.
 * This handler also deals with setting correct response MIME types and returning
 * proper codes, like OK, NOT_FOUND, or SERVER_ERROR.
 */
@ChannelHandler.Sharable
public class RuntimeMonitorHandler extends RedirectHandler<JobManagerGateway> implements WebHandler {

	private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitorHandler.class);

	public static final String WEB_MONITOR_ADDRESS_KEY = "web.monitor.address";

	private final RequestHandler handler;

	private final String allowOrigin;

	public RuntimeMonitorHandler(
			WebMonitorConfig cfg,
			RequestHandler handler,
			GatewayRetriever<JobManagerGateway> retriever,
			CompletableFuture<String> localJobManagerAddressFuture,
			Time timeout) {

		super(localJobManagerAddressFuture, retriever, timeout);
		this.handler = checkNotNull(handler);
		this.allowOrigin = cfg.getAllowOrigin();
	}

	public String[] getPaths() {
		return handler.getPaths();
	}

	@Override
	protected void respondAsLeader(ChannelHandlerContext ctx, Routed routed, JobManagerGateway jobManagerGateway) {
		CompletableFuture<FullHttpResponse> responseFuture;

		try {
			// we only pass the first element in the list to the handlers.
			Map<String, String> queryParams = new HashMap<>();
			for (String key : routed.queryParams().keySet()) {
				queryParams.put(key, routed.queryParam(key));
			}

			Map<String, String> pathParams = new HashMap<>(routed.pathParams().size());
			for (String key : routed.pathParams().keySet()) {
				pathParams.put(key, URLDecoder.decode(routed.pathParams().get(key), ENCODING.toString()));
			}

			queryParams.put(WEB_MONITOR_ADDRESS_KEY, localAddressFuture.get());

<<<<<<< HEAD
			response = handler.handleRequest(pathParams, queryParams, jobManager);
		}
		catch (NotFoundException e) {
			// this should result in a 404 error code (not found)
			ByteBuf message = e.getMessage() == null ? Unpooled.buffer(0)
					: Unpooled.wrappedBuffer(e.getMessage().getBytes(ENCODING));
			response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, message);
			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=" + ENCODING.name());
			response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
			LOG.debug("Error while handling request", e);
		}
		catch (Exception e) {
			byte[] bytes = ExceptionUtils.stringifyException(e).getBytes(ENCODING);
			response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
					HttpResponseStatus.INTERNAL_SERVER_ERROR, Unpooled.wrappedBuffer(bytes));
			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=" + ENCODING.name());
			response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

			LOG.debug("Error while handling request", e);
		}

		response.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN, allowOrigin);
=======
			responseFuture = handler.handleRequest(pathParams, queryParams, jobManagerGateway);

		} catch (Exception e) {
			responseFuture = FutureUtils.completedExceptionally(e);
		}

		responseFuture.whenComplete(
			(HttpResponse httpResponse, Throwable throwable) -> {
				final HttpResponse finalResponse;

				if (throwable != null) {
					LOG.debug("Error while handling request.", throwable);

					Optional<Throwable> optNotFound = ExceptionUtils.findThrowable(throwable, NotFoundException.class);

					if (optNotFound.isPresent()) {
						// this should result in a 404 error code (not found)
						finalResponse = HandlerRedirectUtils.getResponse(HttpResponseStatus.NOT_FOUND, optNotFound.get().getMessage());
					} else {
						finalResponse = HandlerRedirectUtils.getErrorResponse(throwable);
					}
				} else {
					finalResponse = httpResponse;
				}
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

				finalResponse.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN, allowOrigin);
				KeepAliveWrite.flush(ctx, routed.request(), finalResponse);
			});
	}
}
