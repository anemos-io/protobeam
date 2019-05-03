package io.anemos.protobeam;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

public class GoogleCloudRunner extends BlockJUnit4ClassRunner {
  public GoogleCloudRunner(Class<?> clazz) throws InitializationError {
    super(clazz);
  }

  @Override
  protected boolean isIgnored(FrameworkMethod child) {
    String gcpProject = System.getenv("GCP_PROJECT");
    return gcpProject == null || !"".equals(gcpProject);
  }
}
