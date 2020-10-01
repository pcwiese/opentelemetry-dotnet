// <copyright file="OtlpTextWriterExporter.cs" company="OpenTelemetry Authors">
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

namespace OpenTelemetry.Exporter.OpenTelemetryProtocol
{
#if DEBUG
    using System.IO;
    using System.Threading;
    using Google.Protobuf;
    using Grpc.Core;
    using Opentelemetry.Proto.Collector.Trace.V1;

    /// <summary>
    /// FOR DEBUGGING ONLY
    /// Serializes Spans according to the OpenTelemetry Protocol and writes them to a TextWriter.
    /// </summary>
    internal sealed class OtlpTextWriterExporter : OtlpExporter
    {
        /// <summary>
        /// The writer.
        /// </summary>
        private readonly TextWriter writer;

        /// <summary>
        /// Initializes a new instance of the <see cref="OtlpTextWriterExporter" /> class.
        /// </summary>
        /// <param name="writer">The writer.</param>
        public OtlpTextWriterExporter(TextWriter writer)
            : base(
                  new OtlpExporterOptions(),
                  new FileTraceServiceClient(writer))
        {
            this.writer = writer;
        }

        /// <inheritdoc/>
        protected override bool OnShutdown(int timeoutMilliseconds)
        {
            this.writer.Flush();
            return true;
        }

        internal sealed class FileTraceServiceClient : TraceService.ITraceServiceClient
        {
            private readonly TextWriter writer;

            public FileTraceServiceClient(TextWriter writer)
            {
                this.writer = writer;
            }

            public ExportTraceServiceResponse Export(ExportTraceServiceRequest request, Metadata headers = null, System.DateTime? deadline = null, CancellationToken cancellationToken = default)
            {
                // the underlying implementation is reflection based so is slow
                var json = JsonFormatter.Default.Format(request);
                this.writer.WriteLine(json);
                this.writer.Flush();

                // The return value is unchecked by the OtlpExporter
                return null;
            }
        }
    }
}
#endif
