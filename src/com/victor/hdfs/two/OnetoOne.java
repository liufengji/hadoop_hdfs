package com.victor.hdfs.two;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class OnetoOne {
	
	//一致性模型
	@Test
	public void writeFile() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);

		// 2 创建文件输出流
		Path path = new Path("hdfs://hadoop131:8020/user/victor/hello.txt");
		FSDataOutputStream fos = fs.create(path);

		// 3 写数据
		fos.write("hello".getBytes());
		// fos.flush();
		
		//FsDataOutputStream.hflus();		//清理客户端缓冲区数据，被其他client立即可见
		fos.hflush();
		 
		//FsDataOutputStream.hsync();		//清理客户端缓冲区数据，被其他client不能立即可见
		// fos.write("welcome to victor".getBytes());
		// fos.hsync();

		fos.close();
	}

}
