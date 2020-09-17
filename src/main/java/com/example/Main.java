package com.example;

import com.madmk.Actuate;
import com.madmk.SimpleDispatcherFactory;
import com.madmk.configuration.LinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author madmk
 * @date 2020/8/27 20:54
 * @description:
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) throws Exception {
        log.info("创建配置");
        LinkConfiguration linkConfiguration=new LinkConfiguration("192.168.2.125",11111, "example", "","",".*\\..*",1000);
        log.info("创建执行对象");
        Actuate actuate=new Actuate(Main.class.getPackage().getName(),linkConfiguration,new SimpleDispatcherFactory());
        log.info("开始启动");
        actuate.start();
        log.info("启动成功");
    }
}
