package feast.core.job;

public enum Runner {
  DATAFLOW("DataflowRunner"),
  FLINK("FlinkRunner"),
  DIRECT("DirectRunner");

  private final String name;

  Runner(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static Runner fromString(String runner) {
    for (Runner r : Runner.values()) {
      if (r.getName().equals(runner)) {
        return r;
      }
    }
    throw new IllegalArgumentException("Unknown value: " + runner);
  }
}
