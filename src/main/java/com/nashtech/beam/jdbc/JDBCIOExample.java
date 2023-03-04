/**
 * this JDBCIOExample is used for connecting jdbc:mysql database with beam
 * copyright : knoldus Inc.
 * version : 11.0.11
 * @author shashikant
 */
package com.nashtech.beam.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

public class JDBCIOExample {

    public static void main(String[] args) {

        Pipeline p = Pipeline.create();
        PCollection<String> pOutput =p.apply(JdbcIO.<String>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("com.mysql.cj.jdbc.Driver","jdbc:mysql://localhost:3306/database1")
                        .withUsername("root")
                        .withPassword("Shashi@15*"))
                        .withQuery("SELECT name,city,currency FROM product_info WHERE name = ? ")
                .withCoder(StringUtf8Coder.of())
                .withStatementPreparator(new JdbcIO.StatementPreparator() {

                    public void setParameters(PreparedStatement preparedStatement) throws Exception{
                        // TODO Auto-generated method
                        preparedStatement.setString(1,"iphone");
                    }
                })
                .withRowMapper(new JdbcIO.RowMapper<String>() {
                    @Override
                    public String mapRow(ResultSet resultSet) throws Exception {
                      return  resultSet.getString(1) +"," +resultSet.getString(2) +","+ resultSet.getString(3);

                    }
                })
        );
        pOutput.apply(TextIO.write().to("src/main/resources/jdbc.csv").withNumShards(1).withSuffix(".csv"));
                p.run();   // to run the pipeline
    }

}
