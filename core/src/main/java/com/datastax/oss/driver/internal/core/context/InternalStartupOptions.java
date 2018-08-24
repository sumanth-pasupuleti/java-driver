/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.context;

import com.datastax.oss.driver.api.core.MavenCoordinates;
import com.datastax.oss.driver.internal.core.DefaultMavenCoordinates;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class InternalStartupOptions {

  public static final String DRIVER_NAME_KEY = "DRIVER_NAME";
  public static final String DRIVER_VERSION_KEY = "DRIVER_VERSION";

  private static final MavenCoordinates MAVEN_COORDINATES =
      DefaultMavenCoordinates.buildFromResource(
          InternalStartupOptions.class.getResource("/com/datastax/oss/driver/Driver.properties"));

  private final Map<String, String> options;

  public InternalStartupOptions(InternalDriverContext context) {
    this.options = buildOptions(context);
  }

  /**
   * Builds a map of options to send in a Startup message. The default set of options are built here
   * and include {@link com.datastax.oss.protocol.internal.request.Startup#COMPRESSION_KEY} (if the
   * context passed in has a compressor/algorithm set), and the driver's {@link #DRIVER_NAME_KEY}
   * and {@link #DRIVER_VERSION_KEY}. The {@link com.datastax.oss.protocol.internal.request.Startup}
   * constructor will add {@link com.datastax.oss.protocol.internal.request.Startup#COMPRESSION_KEY}
   * to the options if it is not present when an instance of Startup is constructed.
   *
   * <p>NOTE: the InternalDriverContext implementation may choose to override any values setup by
   * default here.
   *
   * @param context InternalDriverContext implementation with an optional compression algorithm.
   * @return Map of Startup Options.
   */
  private Map<String, String> buildOptions(InternalDriverContext context) {
    NullAllowingImmutableMap.Builder<String, String> builder = NullAllowingImmutableMap.builder(3);
    String compressionAlgorithm = context.getCompressor().algorithm();
    if (compressionAlgorithm != null && !compressionAlgorithm.trim().isEmpty()) {
      builder.put(Startup.COMPRESSION_KEY, compressionAlgorithm.trim());
    }
    return builder
        .put(DRIVER_NAME_KEY, MAVEN_COORDINATES.getName())
        .put(DRIVER_VERSION_KEY, MAVEN_COORDINATES.getVersion().toString())
        .build();
  }

  public Map<String, String> getOptions() {
    return options;
  }
}
