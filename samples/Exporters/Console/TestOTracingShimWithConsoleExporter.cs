// <copyright file="TestOTracingShimWithConsoleExporter.cs" company="OpenTelemetry Authors">
// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using OpenTelemetry.Exporter.Console;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Trace.Configuration;

namespace Samples
{
    internal class TestOTracingShimWithConsoleExporter
    {
        internal static object Run(OpenTracingShimOptions options)
        {
            Debugger.Launch();

            // Enable OpenTelemetry and use Console exporter.
            using var openTelemetry = OpenTelemetrySdk.EnableOpenTelemetry(
                builder =>
                {
                    builder
                        .SetResource(Resources.CreateServiceResource("MyServiceName"))
                        .UseConsoleExporter(opt => opt.DisplayAsJson = options.DisplayAsJson);
                });

            Task.Run(
                async () =>
                {
                    await RunOpenTracingInstrumentedRequestAsync().ConfigureAwait(false);
                }).GetAwaiter().GetResult();

            Console.WriteLine("Press Enter key to exit.");

            return null;
        }

        private static async Task RunOpenTracingInstrumentedRequestAsync()
        {
            // Start a local gRPC server
            using var foobarServer = new FoobarServer();

            // An implementation of the OpenTracing ITracer
            var openTracingTracer = new OpenTelemetry.Shims.OpenTracing.TracerShim(
                TracerProvider.GetTracer("MyCompany.MyProduct.MyWebServer"),
                new OpenTelemetry.Context.Propagation.TraceContextFormat());

            // This is an OpenTracing instrumented client interceptor
            var interceptor = new OpenTracing.Contrib.Grpc.Interceptors.ClientTracingInterceptor.Builder(openTracingTracer)
                .Build();

            var channel = new Channel(foobarServer.Uri.OriginalString, ChannelCredentials.Insecure);
            var client = new Foobar.FoobarClient(channel.Intercept(interceptor));
            var call = client.DuplexStreaming();

            foreach (var request in Enumerable.Range(1, 5).Select(x => new FoobarRequest { Message = "foo" }))
            {
                await call.RequestStream.WriteAsync(request).ConfigureAwait(false);
            }

            await call.RequestStream.CompleteAsync().ConfigureAwait(false);

            while (await call.ResponseStream.MoveNext().ConfigureAwait(false))
            {
            }
        }

        /// <summary>
        /// A Grpc.Core based in process gRPC server.
        /// </summary>
        private sealed class FoobarServer : Server, IDisposable
        {
            /// <summary>
            /// The server.
            /// </summary>
            private readonly Server server;

            public FoobarServer()
            {
                // Start an in process Grpc.Core based service
                // Disable SO_REUSEPORT to prevent https://github.com/grpc/grpc/issues/10755
                this.server = new Server(new[] { new ChannelOption(ChannelOptions.SoReuseport, 0) })
                {
                    Services = { Foobar.BindService(new FoobarImpl()) },
                    Ports = { { "localhost", ServerPort.PickUnused, ServerCredentials.Insecure } },
                };

                this.server.Start();

                this.Uri = new Uri("dns:localhost:" + this.server.Ports.Single().BoundPort);
            }

            public Uri Uri { get; }

            /// <inheritdoc/>
            public void Dispose()
            {
                this.server.ShutdownAsync().GetAwaiter().GetResult();
            }

            /// <summary>
            /// Test implementation of foobar.
            /// </summary>
            private class FoobarImpl : Foobar.FoobarBase
            {
                /// <summary>
                /// The default response message.
                /// </summary>
                private static readonly FoobarResponse DefaultResponseMessage = new FoobarResponse { Message = "bar" };

                /// <inheritdoc/>
                public override async Task DuplexStreaming(IAsyncStreamReader<FoobarRequest> requestStream, IServerStreamWriter<FoobarResponse> responseStream, ServerCallContext context)
                {
                    while (await requestStream.MoveNext().ConfigureAwait(false))
                    {
                    }

                    foreach (var response in Enumerable.Range(1, 5).Select(x => DefaultResponseMessage))
                    {
                        await responseStream.WriteAsync(response).ConfigureAwait(false);
                    }
                }
            }
        }
    }
}
