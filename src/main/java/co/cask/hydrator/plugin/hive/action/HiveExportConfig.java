package co.cask.hydrator.plugin.hive.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.hydrator.plugin.hive.action.common.HiveConfig;

import javax.annotation.Nullable;

/**
 * Hive Export config
 */
public class HiveExportConfig extends HiveConfig {
  public static final String DELIMITER = "delimiter";
  public static final String PATH = "path";
  public static final String OVERWRITE = "overwrite";

  @Name(DELIMITER)
  @Description("Delimiter in the exported file. Values in each column is separated by this delimiter while writing to" +
    " output file")
  @Nullable
  @Macro
  public String delimiter;

  @Name(PATH)
  @Description("HDFS directory path where exported data will be written")
  @Macro
  public String path;

  @Name(OVERWRITE)
  @Description("If HDFS directory already exists should it be overwritten?")
  @Macro
  @Nullable
  public String overwrite;

  public HiveExportConfig(String connectionString, String user, String password, String statement,
                          String delimiter, String path, String overwrite) {
    super(connectionString, user, password, statement);
    this.delimiter = delimiter;
    this.path = path;
    this.overwrite = overwrite;
  }
}
