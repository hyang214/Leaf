package com.sankuai.inf.leaf.segment.dao;

import com.sankuai.inf.leaf.segment.model.LeafAlloc;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * sql存储，替代mapper
 */
public interface IDAllocMapper {

    /**
     * 查询全部的配置
     * @return
     */
    @Select("SELECT biz_tag, max_id, step, update_time FROM leaf_alloc")
    @Results(value = {
            @Result(column = "biz_tag", property = "key"),
            @Result(column = "max_id", property = "maxId"),
            @Result(column = "step", property = "step"),
            @Result(column = "update_time", property = "updateTime")
    })
    List<LeafAlloc> getAllLeafAllocs();

    @Select("SELECT biz_tag, max_id, step FROM leaf_alloc WHERE biz_tag = #{tag}")
    @Results(value = {
            @Result(column = "biz_tag", property = "key"),
            @Result(column = "max_id", property = "maxId"),
            @Result(column = "step", property = "step")
    })
    LeafAlloc getLeafAlloc(@Param("tag") String tag);

    /**
     * 数据库已经分配的key向前移动step步
     *
     * @param tag 业务key
     */
    @Update("UPDATE leaf_alloc SET max_id = max_id + step WHERE biz_tag = #{tag}")
    void updateMaxId(@Param("tag") String tag);

    /**
     * 和updateMaxId类型，但是带上step参数，而不是用数据库中存储的
     *
     * @param leafAlloc
     *      key: 业务key
     *      step: 本次新增步长
     */
    @Update("UPDATE leaf_alloc SET max_id = max_id + #{step} WHERE biz_tag = #{key}")
    void updateMaxIdByCustomStep(@Param("leafAlloc") LeafAlloc leafAlloc);

    /**
     * 查询全部业务标识
     *
      * @return
     */
    @Select("SELECT biz_tag FROM leaf_alloc")
    List<String> getAllTags();
}
