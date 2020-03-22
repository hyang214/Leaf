package com.sankuai.inf.leaf.segment.dao;

import com.sankuai.inf.leaf.segment.model.LeafAlloc;

import java.util.List;

public interface IDAllocDao {

     /**
      * 查询全部的配置
      * @return
      */
     List<LeafAlloc> getAllLeafAllocs();

     /**
      * 更新数据库已经分配最大id，并且返回分配对象
      * @param tag
      * @return
      */
     LeafAlloc updateMaxIdAndGetLeafAlloc(String tag);

     /**
      * 更新数据库已经分配最大id，并且返回分配对象
      * 与 {@link #updateMaxIdAndGetLeafAlloc} 相比，多了传入的step参数
      *
      * @param leafAlloc
      * @return
      */
     LeafAlloc updateMaxIdByCustomStepAndGetLeafAlloc(LeafAlloc leafAlloc);

     /**
      * 查询全部业务标识
      *
      * @return
      */
     List<String> getAllTags();
}
