package com.liuapi.office.helper.reportsync;

import com.google.common.base.*;
import com.google.common.io.Files;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SyncParser {
    /**
     * 解析 middleware.properties
     * @throws IOException
     */
    @Test
    public void parseO45Mq() throws IOException {
        parseMqTopic("E:\\reportsync\\middleware_o45.properties",
                "E:\\reportsync\\bb_tmp_topics_o45.dml");
    }
    @Test
    public void parseAm4Mq() throws IOException {
        parseMqTopic("E:\\reportsync\\middleware_am4.properties",
                "E:\\reportsync\\bb_tmp_topics_am4.dml");
    }
    @Test
    public void parseO45Uft() throws IOException{
        parseUft("E:\\reportsync\\kafka_compt_o45.xml"
                ,"E:\\reportsync\\source\\ALL_TABLES_202404030958.csv"
                ,"E:\\reportsync\\bb_tmp_ufts_o45.dml");
    }
    @Test
    public void parseAm4Uft() throws IOException{
        parseUft("E:\\reportsync\\kafka_compt_am4.xml"
                ,"E:\\reportsync\\source\\tables_202404081030.csv"
                ,"E:\\reportsync\\bb_tmp_ufts_am4.dml");
    }

    private void parseMqTopic(String mqTopicPath, String exportPath) throws IOException {
        Map<String, String> topicKV = parseMqTopic(mqTopicPath);
        List<String> sqls = topicKV.entrySet().stream()
                .map(entry -> "INSERT INTO bb_tmp_topics (topic_name, real_topic_name) VALUES ('" + entry.getKey() + "', '" + entry.getValue() + "');")
                .toList();
        String sqlLine = Joiner.on('\n').skipNulls().join(sqls);
        sqlLine = "truncate table bb_tmp_topics;\n" + sqlLine;
        Files.asCharSink(new File(exportPath), Charsets.UTF_8)
                .write(sqlLine);
    }
    /**
     * 解析kafka_compt.xml 以及 实体表名列表
     *
     * @throws IOException
     */
    private void parseUft(String uftPath, String realTablePath, String outputPath) throws IOException {
        List<String> uftTableNames = parseUft(uftPath);
        List<String> realTableNames = parseTables(realTablePath);
        // uft的表一定能找到对应的实体表
        List<String> sqls = uftTableNames.stream()
                .map(
                        uft -> {
                            String realTableName = "";
                            int maxCommonSize = 0;
                            for (String tableName : realTableNames) {
                                // 前缀是否相同
                                String uftPrefix = uft.substring(0, uft.indexOf("_"));
                                String tablePrefix = tableName.substring(0, uft.indexOf("_"));
                                if (!Objects.equals(uftPrefix, tablePrefix)) {
                                    // 前缀不一样,直接返回
                                    continue;
                                }
                                String commonPart = Strings.commonSuffix(uft, tableName);
                                if (commonPart.length() > maxCommonSize) {
                                    maxCommonSize = commonPart.length();
                                    realTableName = tableName;
                                } else if (commonPart.length() == maxCommonSize) {
                                    if (realTableName.length() > tableName.length()) {
                                        realTableName = tableName;
                                    }
                                }
                            }
                            if (uft.equals("gg_tadatayx")) {
                                realTableName="gg_ttadata";
                            }
                            return "INSERT INTO bb_tmp_ufts (uft_table_name, real_table_name) VALUES ('" + uft + "','" + realTableName + "');";
                        }
                )
                .toList();

        // 这里再建立下uft表名与实体表名的关联关系
        String sqlLine = Joiner.on('\n').skipNulls().join(sqls);
        sqlLine = "truncate table bb_tmp_ufts;\n" + sqlLine;
        Files.asCharSink(new File(outputPath), Charsets.UTF_8)
                .write(sqlLine);
    }

    private List<String> parseTables(String filePath) throws IOException {
        List<String> lines = Files.readLines(new File(filePath), Charsets.UTF_8);
        return lines.stream()
                .skip(1)
                .map(String::toLowerCase)
                .filter(line -> !line.startsWith("his"))
                .filter(line -> !line.contains("_ti_"))
                .toList();
    }

    private List<String> parseUft(String filePath) throws IOException {
        List<String> lines = Files.readLines(new File(filePath), Charsets.UTF_8);
        return lines.stream()
                .filter(line -> line.contains("table name"))
                .map(line -> Splitter.on("\"").splitToList(line).get(1))
                .toList();
    }

    /**
     * SELECT t.topic_name,t.table_name,t.c_job_name,g.TARGET_TABLE_NAME
     * FROM bb_tmqtableconfig t
     * LEFT JOIN (
     * SELECT table_name, max(TARGET_TABLE_NAME) AS TARGET_TABLE_NAME FROM bb_tdataconvertconfig GROUP BY table_name
     * ) g ON t.table_name = g.table_name
     * ORDER by t.SERIAL_NO ASC;
     *
     * @throws IOException
     */

    private Map<String, String> parseMqTopic(String filePath) throws IOException {
        List<String> lines = Files.readLines(new File(filePath), Charsets.UTF_8);
        return lines.stream()
                .filter(line -> line.contains("routingKey"))
                .map(line -> Splitter.on("=").splitToList(line).get(1))
                .flatMap(topicLine -> Splitter.on(",").splitToStream(topicLine))
                .distinct()
                .collect(Collectors.toMap(
                        topic -> topic.replaceAll("\\.\\*", ""),
                        Function.identity()
                ));
    }

}
