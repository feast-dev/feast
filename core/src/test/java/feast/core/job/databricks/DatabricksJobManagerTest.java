/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.core.job.databricks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.services.dataflow.Dataflow;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.job.databricks.DatabricksJobManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import feast.core.model.Job;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;

public class DatabricksJobManagerTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Mock
    private Job job;

    private Map<String, String> runnerConfigOptions;
    private DatabricksJobManager dbJobManager;
    private String token;

    @Before
    public void setUp() {
        initMocks(this);
        runnerConfigOptions = new HashMap<>();
        runnerConfigOptions.put("databricksService", "https://adb-8918595472279780.0.azuredatabricks.net");
        token = "YOUR TOKEN";
        MetricsProperties metricsProperties = new MetricsProperties();
        metricsProperties.setEnabled(false);
        dbJobManager = new DatabricksJobManager(runnerConfigOptions, metricsProperties, token);
        dbJobManager = spy(dbJobManager);
    }

    @Test
    public void testGetCalltoDatabricks() throws NoSuchFieldException {
//        FieldSetter.setField(job, job.getClass().getDeclaredField("extId"), "1");
        Mockito.when(job.getExtId()).thenReturn("1");

        dbJobManager.getJobStatus(job);
    }
}
