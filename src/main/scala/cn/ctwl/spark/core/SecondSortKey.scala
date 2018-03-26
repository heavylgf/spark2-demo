package cn.ctwl.spark.core

import java.io.Serializable


/**
 * 自定义二次排序的key
 * 首先在自定义key里面，定义需要进行排序的列
 */
class SecondSortKey(val first: Int, val second: Int) 
    extends Ordered[SecondSortKey] with Serializable {
  
  def compare(that: SecondSortKey): Int = {
    if(this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
 
}