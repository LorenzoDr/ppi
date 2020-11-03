/**
 * 
 */
package it.kazaam.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author salvatore
 *
 */
@Configuration
public class SparkConfig {
	
    @Value("ProvaSpark")
    private String appName;
    
    @Value("local[*]")
    private String masterUri;
    
    @Bean
    public SparkConf conf() {
        return new SparkConf()
        		.setAppName(appName)
        		.setMaster(masterUri)
        		.set("spark.driver.memory", "3G")
				.set("spark.driver.allowMultipleContexts","true");
    }
 
    @Bean
    public JavaSparkContext sc() {
        return new JavaSparkContext(conf());
    }


}
