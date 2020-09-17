package com.example.dispatcher;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.madmk.annotation.Dispatcher;
import com.madmk.annotation.Worker;

import java.util.List;
import java.util.regex.Pattern;

/**
 * @author madmk
 * @date 2020/8/27 20:58
 * @description: 测试用调度器
 */
@Dispatcher
public class TestDispatcher {

    @Worker
    public void t1(CanalEntry.Entry entry,CanalEntry.RowChange change){
        System.out.println("============= t1接收到事件 ============");
        CanalEntry.Header header=entry.getHeader();
        if(header!=null){
            System.out.println("HEADER-LogfileName:"+header.getLogfileName());
            System.out.println("HEADER-SchemaName:"+header.getSchemaName());
            System.out.println("HEADER-TableName:"+header.getTableName());
            System.out.println("HEADER-Gtid:"+header.getGtid());
            System.out.println("HEADER-ServerenCode:"+header.getServerenCode());
            System.out.println("HEADER-InitializationErrorString:"+header.getInitializationErrorString());
            System.out.println("HEADER-LogfileOffset:"+header.getLogfileOffset());
            System.out.println("HEADER-EventType:"+header.getEventType());
            System.out.println("HEADER-DefaultInstanceForType:"+header.getDefaultInstanceForType());
        }
        System.out.println("EntryType:" + entry.getEntryType());
        System.out.println("DefaultInstanceForType:" + entry.getDefaultInstanceForType());
        System.out.println("SQL:" + change.getSql());
        System.out.println("IsDdl:" + change.getIsDdl());
        System.out.println("RowDatasCount:" + change.getRowDatasCount());

        for (CanalEntry.RowData rowData : change.getRowDatasList()) {
            List<CanalEntry.Column> beforeColumns=rowData.getBeforeColumnsList();
            List<CanalEntry.Column> afterColumns= rowData.getAfterColumnsList();
            if(beforeColumns!=null&&beforeColumns.size()>0){
                System.out.println("--- beforeColumns");
                beforeColumns.forEach(column->System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated()));
                System.out.println("---");
            }
            if(afterColumns!=null&&afterColumns.size()>0){
                System.out.println("--- afterColumns");
                afterColumns.forEach(column->System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated()));
                System.out.println("---");
            }
        }
        System.out.println("===================================");
    }


    @Worker(type= CanalEntry.EventType.INSERT,schema="test",table = "user")
    public void t2(CanalEntry.RowChange change,CanalEntry.Entry entry){
        System.out.println("============= t1接收到事件 ============");
        CanalEntry.Header header=entry.getHeader();
        if(header!=null){
            System.out.println("HEADER-LogfileName:"+header.getLogfileName());
            System.out.println("HEADER-SchemaName:"+header.getSchemaName());
            System.out.println("HEADER-TableName:"+header.getTableName());
            System.out.println("HEADER-Gtid:"+header.getGtid());
            System.out.println("HEADER-ServerenCode:"+header.getServerenCode());
            System.out.println("HEADER-InitializationErrorString:"+header.getInitializationErrorString());
            System.out.println("HEADER-LogfileOffset:"+header.getLogfileOffset());
            System.out.println("HEADER-EventType:"+header.getEventType());
            System.out.println("HEADER-DefaultInstanceForType:"+header.getDefaultInstanceForType());
        }
        System.out.println("EntryType:" + entry.getEntryType());
        System.out.println("DefaultInstanceForType:" + entry.getDefaultInstanceForType());
        System.out.println("SQL:" + change.getSql());
        System.out.println("IsDdl:" + change.getIsDdl());
        System.out.println("RowDatasCount:" + change.getRowDatasCount());

        for (CanalEntry.RowData rowData : change.getRowDatasList()) {
            List<CanalEntry.Column> beforeColumns=rowData.getBeforeColumnsList();
            List<CanalEntry.Column> afterColumns= rowData.getAfterColumnsList();
            if(beforeColumns!=null&&beforeColumns.size()>0){
                System.out.println("--- beforeColumns");
                beforeColumns.forEach(column->System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated()));
                System.out.println("---");
            }
            if(afterColumns!=null&&afterColumns.size()>0){
                System.out.println("--- afterColumns");
                afterColumns.forEach(column->System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated()));
                System.out.println("---");
            }
        }
        System.out.println("===================================");
    }

    @Worker(type= CanalEntry.EventType.DELETE,schema="test",table = "user")
    public void t3(CanalEntry.RowChange change,CanalEntry.Entry entry){
        System.out.println("============= t3接收到事件 ============");
        CanalEntry.Header header=entry.getHeader();
        if(header!=null){
            System.out.println("HEADER-LogfileName:"+header.getLogfileName());
            System.out.println("HEADER-SchemaName:"+header.getSchemaName());
            System.out.println("HEADER-TableName:"+header.getTableName());
            System.out.println("HEADER-Gtid:"+header.getGtid());
            System.out.println("HEADER-ServerenCode:"+header.getServerenCode());
            System.out.println("HEADER-InitializationErrorString:"+header.getInitializationErrorString());
            System.out.println("HEADER-LogfileOffset:"+header.getLogfileOffset());
            System.out.println("HEADER-EventType:"+header.getEventType());
            System.out.println("HEADER-DefaultInstanceForType:"+header.getDefaultInstanceForType());
        }
        System.out.println("EntryType:" + entry.getEntryType());
        System.out.println("DefaultInstanceForType:" + entry.getDefaultInstanceForType());
        System.out.println("SQL:" + change.getSql());
        System.out.println("IsDdl:" + change.getIsDdl());
        System.out.println("RowDatasCount:" + change.getRowDatasCount());

        for (CanalEntry.RowData rowData : change.getRowDatasList()) {
            List<CanalEntry.Column> beforeColumns=rowData.getBeforeColumnsList();
            List<CanalEntry.Column> afterColumns= rowData.getAfterColumnsList();
            if(beforeColumns!=null&&beforeColumns.size()>0){
                System.out.println("--- beforeColumns");
                beforeColumns.forEach(column->System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated()));
                System.out.println("---");
            }
            if(afterColumns!=null&&afterColumns.size()>0){
                System.out.println("--- afterColumns");
                afterColumns.forEach(column->System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated()));
                System.out.println("---");
            }
        }
        System.out.println("===================================");
    }

}
