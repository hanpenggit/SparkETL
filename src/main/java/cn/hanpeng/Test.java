package cn.hanpeng;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class Test {
    public static void main(String[] args) {
        long start=System.currentTimeMillis();
        log.debug("debug");
        log.info("info");
        log.warn("warn");
        log.error("error");
        long end=System.currentTimeMillis();
        log.info("total [{}] ms",(end-start));
    }
}
