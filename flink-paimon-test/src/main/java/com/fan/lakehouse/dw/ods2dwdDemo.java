package com.fan.lakehouse.dw;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;

public class ods2dwdDemo {

    public static void writeToDwd() {
        // create environments of both APIs
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        //env.enableCheckpointing(60000);
        TableEnvironment tableEnv = TableEnvironment.create(env.getConfiguration());


        // create paimon catalog
        tableEnv.executeSql("CREATE CATALOG my_catalog WITH ('type' = 'paimon', 'warehouse'='file:/Users/fanzhe/Downloads/paimon')");
        tableEnv.executeSql("USE CATALOG my_catalog");
        tableEnv.executeSql("insert into ods.ods_gaia_workpoint_order_ss_df\n" +
                "      /*+ OPTIONS('sink.parallelism'='2','write-buffer-size'='1024m','taskmanager.memory.flink.size'='4096','sink.partition-shuffle' = 'true') */\n" +
                "      select                            tt.id,\n" +
                "                                        tt.app_id,\n" +
                "                                        tt.order_no,\n" +
                "                                        tt.standard_order_no,\n" +
                "                                        tt.ent_id,\n" +
                "                                        tt.ent_name,\n" +
                "                                        tt.ent_logo,\n" +
                "                                        tt.project_id,\n" +
                "                                        tt.backup_project_id,\n" +
                "                                        tt.project_name,\n" +
                "                                        tt.workspace_id,\n" +
                "                                        tt.workspace_name,\n" +
                "                                        tt.agency_id,\n" +
                "                                        tt.agency_ou_id,\n" +
                "                                        tt.agency_name,\n" +
                "                                        tt.agency_type,\n" +
                "                                        tt.team_id,\n" +
                "                                        tt.team_name,\n" +
                "                                        tt.team_owner_worker_id,\n" +
                "                                        tt.team_owner_user_id,\n" +
                "                                        tt.team_owner_name,\n" +
                "                                        tt.team_owner_face_url,\n" +
                "                                        tt.group_id,\n" +
                "                                        tt.group_name,\n" +
                "                                        tt.group_owner_worker_id,\n" +
                "                                        tt.group_owner_user_id,\n" +
                "                                        tt.group_owner_name,\n" +
                "                                        tt.group_owner_face_url,\n" +
                "                                        tt.worker_id,\n" +
                "                                        tt.worker_user_id,\n" +
                "                                        tt.worker_name,\n" +
                "                                        tt.worker_face_url,\n" +
                "                                        tt.source,\n" +
                "                                        tt.profession_id,\n" +
                "                                        tt.profession_name,\n" +
                "                                        tt.task_date_start,\n" +
                "                                        tt.task_date_end,\n" +
                "                                        tt.record_date,\n" +
                "                                        tt.workpoint_type,\n" +
                "                                        tt.record_at,\n" +
                "                                        tt.status,\n" +
                "                                        tt.total_amount,\n" +
                "                                        tt.original_total_amount,\n" +
                "                                        tt.total_work_amount,\n" +
                "                                        tt.quantity,\n" +
                "                                        tt.quantity_unit,\n" +
                "                                        tt.total_overtime_amount,\n" +
                "                                        tt.overtime_quantity,\n" +
                "                                        tt.overtime_quantity_unit,\n" +
                "                                        tt.attendance_hour_count,\n" +
                "                                        tt.dt,\n" +
                "\t\t\t\t\ttt.create_at,\n" +
                "                                        tt.update_at\n" +
                "          from\n" +
                "      (select ss.*,\n" +
                "                         TO_TIMESTAMP(DATE_FORMAT(coalesce(create_at,now()), 'yyyy-MM-dd') , 'yyyy-MM-dd')  as dt,\n" +
                "                         row_number() over (partition by ss.order_no,ss.status order by DATE_FORMAT(ss.create_at, '%Y-%m-%d') desc) as rn\n" +
                "                  from (select\n" +
                "                                        wo.id,\n" +
                "                                        wo.app_id,\n" +
                "                                        wo.order_no,\n" +
                "                                        wo.standard_order_no,\n" +
                "                                        wo.ent_id,\n" +
                "                                        wo.ent_name,\n" +
                "                                        wo.ent_logo,\n" +
                "                                        wo.project_id,\n" +
                "                                        wo.backup_project_id,\n" +
                "                                        wo.project_name,\n" +
                "                                        wo.workspace_id,\n" +
                "                                        wo.workspace_name,\n" +
                "                                        wo.agency_id,\n" +
                "                                        wo.agency_ou_id,\n" +
                "                                        wo.agency_name,\n" +
                "                                        wo.agency_type,\n" +
                "                                        wo.team_id,\n" +
                "                                        wo.team_name,\n" +
                "                                        wo.team_owner_worker_id,\n" +
                "                                        wo.team_owner_user_id,\n" +
                "                                        wo.team_owner_name,\n" +
                "                                        wo.team_owner_face_url,\n" +
                "                                        wo.group_id,\n" +
                "                                        wo.group_name,\n" +
                "                                        wo.group_owner_worker_id,\n" +
                "                                        wo.group_owner_user_id,\n" +
                "                                        wo.group_owner_name,\n" +
                "                                        wo.group_owner_face_url,\n" +
                "                                        wo.worker_id,\n" +
                "                                        wo.worker_user_id,\n" +
                "                                        wo.worker_name,\n" +
                "                                        wo.worker_face_url,\n" +
                "                                        wo.source,\n" +
                "                                        wo.profession_id,\n" +
                "                                        wo.profession_name,\n" +
                "                                        wo.task_date_start,\n" +
                "                                        wo.task_date_end,\n" +
                "                                        wo.record_date,\n" +
                "                                        wo.workpoint_type,\n" +
                "                                        wo.record_at,\n" +
                "                                       coalesce(wol.status, wo.status) as status,\n" +
                "                                        wo.total_amount,\n" +
                "                                        wo.original_total_amount,\n" +
                "                                        wo.total_work_amount,\n" +
                "                                        wo.quantity,\n" +
                "                                        wo.quantity_unit,\n" +
                "                                        wo.total_overtime_amount,\n" +
                "                                        wo.overtime_quantity,\n" +
                "                                        wo.overtime_quantity_unit,\n" +
                "                                        wo.attendance_hour_count,\n" +
                "                                        wo.creator_type,\n" +
                "                                        wo.push_chain_status,\n" +
                "                                        wo.push_clear_status,\n" +
                "                                        wo.push_regulate_status,\n" +
                "                                        wo.check_status,\n" +
                "                                        wo.work_hour_quantity,\n" +
                "                                        wo.work_hour_quantity_unit,\n" +
                "                                        wo.actual_quantity,\n" +
                "                                        wo.actual_quantity_unit,\n" +
                "                                        wo.is_delete,\n" +
                "                                        wol.create_at,\n" +
                "                                        wo.update_at,\n" +
                "                                        wo.finish_at,\n" +
                "                                        wo.outside_attendance_hour_count,\n" +
                "                                        wo.outside_attendance_last_update_at,\n" +
                "                                        wo.source_type,\n" +
                "                                        wo.data_version,\n" +
                "                                        wo.update_version\n" +
                "                            from(select id,\n" +
                "                                        app_id,\n" +
                "                                        order_no,\n" +
                "                                        standard_order_no,\n" +
                "                                        ent_id,\n" +
                "                                        ent_name,\n" +
                "                                        ent_logo,\n" +
                "                                        project_id,\n" +
                "                                        backup_project_id,\n" +
                "                                        project_name,\n" +
                "                                        workspace_id,\n" +
                "                                        workspace_name,\n" +
                "                                        agency_id,\n" +
                "                                        agency_ou_id,\n" +
                "                                        agency_name,\n" +
                "                                        agency_type,\n" +
                "                                        team_id,\n" +
                "                                        team_name,\n" +
                "                                        team_owner_worker_id,\n" +
                "                                        team_owner_user_id,\n" +
                "                                        team_owner_name,\n" +
                "                                        team_owner_face_url,\n" +
                "                                        group_id,\n" +
                "                                        group_name,\n" +
                "                                        group_owner_worker_id,\n" +
                "                                        group_owner_user_id,\n" +
                "                                        group_owner_name,\n" +
                "                                        group_owner_face_url,\n" +
                "                                        worker_id,\n" +
                "                                        worker_user_id,\n" +
                "                                        worker_name,\n" +
                "                                        worker_face_url,\n" +
                "                                        source,\n" +
                "                                        profession_id,\n" +
                "                                        profession_name,\n" +
                "                                        task_date_start,\n" +
                "                                        task_date_end,\n" +
                "                                        record_date,\n" +
                "                                        workpoint_type,\n" +
                "                                        record_at,\n" +
                "                                        status,\n" +
                "                                        total_amount,\n" +
                "                                        original_total_amount,\n" +
                "                                        total_work_amount,\n" +
                "                                        quantity,\n" +
                "                                        quantity_unit,\n" +
                "                                        total_overtime_amount,\n" +
                "                                        overtime_quantity,\n" +
                "                                        overtime_quantity_unit,\n" +
                "                                        attendance_hour_count,\n" +
                "                                        creator_type,\n" +
                "                                        push_chain_status,\n" +
                "                                        push_clear_status,\n" +
                "                                        push_regulate_status,\n" +
                "                                        check_status,\n" +
                "                                        work_hour_quantity,\n" +
                "                                        work_hour_quantity_unit,\n" +
                "                                        actual_quantity,\n" +
                "                                        actual_quantity_unit,\n" +
                "                                        is_delete,\n" +
                "                                        create_at,\n" +
                "                                        update_at,\n" +
                "                                        finish_at,\n" +
                "                                        outside_attendance_hour_count,\n" +
                "                                        outside_attendance_last_update_at,\n" +
                "                                        source_type,\n" +
                "                                        data_version,\n" +
                "                                        update_version\n" +
                "                                 from ods.ods_gaia_workpoint_order_si\n" +
                "                        where is_delete = 0) wo\n" +
                "                           LEFT JOIN (select workpoint_order_no,\n" +
                "                                             status,\n" +
                "                                             row_number() over (partition by DATE_FORMAT(create_at, '%Y-%m-%d'),workpoint_order_no,status order by create_at desc) as rn,\n" +
                "                                             create_at\n" +
                "                                      from ods.ods_gaia_workpoint_order_log_si\n" +
                "                                      where is_delete = 0\n" +
                "                                      ) wol\n" +
                "                                     ON wol.workpoint_order_no = wo.order_no\n" +
                "                      union all\n" +
                "      select id,\n" +
                "                                        app_id,\n" +
                "                                        order_no,\n" +
                "                                        standard_order_no,\n" +
                "                                        ent_id,\n" +
                "                                        ent_name,\n" +
                "                                        ent_logo,\n" +
                "                                        project_id,\n" +
                "                                        backup_project_id,\n" +
                "                                        project_name,\n" +
                "                                        workspace_id,\n" +
                "                                        workspace_name,\n" +
                "                                        agency_id,\n" +
                "                                        agency_ou_id,\n" +
                "                                        agency_name,\n" +
                "                                        agency_type,\n" +
                "                                        team_id,\n" +
                "                                        team_name,\n" +
                "                                        team_owner_worker_id,\n" +
                "                                        team_owner_user_id,\n" +
                "                                        team_owner_name,\n" +
                "                                        team_owner_face_url,\n" +
                "                                        group_id,\n" +
                "                                        group_name,\n" +
                "                                        group_owner_worker_id,\n" +
                "                                        group_owner_user_id,\n" +
                "                                        group_owner_name,\n" +
                "                                        group_owner_face_url,\n" +
                "                                        worker_id,\n" +
                "                                        worker_user_id,\n" +
                "                                        worker_name,\n" +
                "                                        worker_face_url,\n" +
                "                                        source,\n" +
                "                                        profession_id,\n" +
                "                                        profession_name,\n" +
                "                                        task_date_start,\n" +
                "                                        task_date_end,\n" +
                "                                        record_date,\n" +
                "                                        workpoint_type,\n" +
                "                                        record_at,\n" +
                "                                        status,\n" +
                "                                        total_amount,\n" +
                "                                        original_total_amount,\n" +
                "                                        total_work_amount,\n" +
                "                                        quantity,\n" +
                "                                        quantity_unit,\n" +
                "                                        total_overtime_amount,\n" +
                "                                        overtime_quantity,\n" +
                "                                        overtime_quantity_unit,\n" +
                "                                        attendance_hour_count,\n" +
                "                                        creator_type,\n" +
                "                                        push_chain_status,\n" +
                "                                        push_clear_status,\n" +
                "                                        push_regulate_status,\n" +
                "                                        check_status,\n" +
                "                                        work_hour_quantity,\n" +
                "                                        work_hour_quantity_unit,\n" +
                "                                        actual_quantity,\n" +
                "                                        actual_quantity_unit,\n" +
                "                                        is_delete,\n" +
                "                                        create_at,\n" +
                "                                        update_at,\n" +
                "                                        finish_at,\n" +
                "                                        outside_attendance_hour_count,\n" +
                "                                        coalesce(outside_attendance_last_update_at,now()) as outside_attendance_last_update_at,\n" +
                "                                        source_type,\n" +
                "                                        data_version,\n" +
                "                                        update_version\n" +
                "                        from ods.ods_gaia_workpoint_order_si\n" +
                "                        where is_delete = 0\n" +
                "                      ) ss)tt\n" +
                "                  WHERE tt.rn=1;");
    }
}
