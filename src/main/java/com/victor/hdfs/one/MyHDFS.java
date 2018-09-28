package com.victor.hdfs.one;
 
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class MyHDFS {

	//HDFS获取文件系统
	@Test
	public void initHDFS() throws Exception {
		//1.创建对象
		Configuration configuretion = new Configuration();
		
		//2.设置参数
		configuretion.set("fs.defaultFS","hdfs://hadoop102:9000");
		configuretion.set("dfs.replication", "3");
		
		//3获取文件系统
		FileSystem fs = FileSystem.get(configuretion);
		
		//4打印
		System.out.println(fs.toString());
	}
	
	
	//HDFS文件上传
	@Test
	public void putFileToHDFS() throws Exception {
		
		//1.创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "victor");
		
		//2.创建要上传文件所在的本地路径
		Path src = new Path("e:/hello.txt");
		
		//3.创建要上传到hdfs的目录路径
		Path dst = new Path("hdfs://hadoop102:9000/user/victor/hello.txt");
		
		//4.拷贝文件
		fs.copyFromLocalFile(src, dst);
		
		//5.关闭资源
		fs.close();
		 
	}	
	
	//hdfs文件下载
	@Test
	public void getFileFromHDFS() throws Exception {
		
		//1.创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"victor");
		
		//2.下载文件
		fs.copyToLocalFile(true, new Path("hdfs://hadoop102:9000/user/victor/hello.txt"), new Path("e:/Hello.txt"), true);
		
		//3.关闭资源
		fs.close();
	}
	
	/*
	 * HDFS目录创建
	 */
	@Test
	public void mkdirAtHDFS() throws Exception {
		//1.创建配置信息文件
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"victor");
		
		//2.创建目录
		fs.mkdirs(new Path("hdfs://hadoop102:9000/user/victor"));
		
		//3.关闭资源
		fs.close();
	}
	
	/*
	 * HDFS文件夹删除
	 */
	@Test
	public void deleteAtHDFS() throws Exception {
		//1 创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"victor");
		//2.删除文件夹 ，如果是非空文件夹，参数2必须给值 true
		fs.delete(new Path("hdfs://hadoop102:9000/user/victor"),true);
		
		//3.关闭资源
		fs.close();
		
	}
	
	/*
	 * HDFS文件名更改
	 */
	@Test
	public void renameAtHDFS() throws Exception {
		//1.创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"victor");
		
		//2.重命名文件或文件夹
		fs.rename(new Path("hdfs://hadoop102:9000/user/victor/hello.txt"), new Path("hdfs://hadoop102:9000/user/victor/hellonihao.txt"));
		
		//3.关闭资源
		fs.close();
		
	}
	
	/*
	 * HDFS文件详情查看
	 */
	@Test
	public void readListFiles() throws Exception {
		//创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"victor");
		
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()){
			LocatedFileStatus next = listFiles.next();
			
			System.out.println(next.getPath().getName());
			System.out.println(next.getBlockSize());
			
			BlockLocation[] blockLocations = next.getBlockLocations();
			
			for (BlockLocation b1 : blockLocations) {
				System.out.println("block-offset"+b1.getOffset());
				String[] hosts = b1.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			
			System.out.println("-----------------------------");
			
		}
		
	}
	
	/*
	 * HDFS文件夹查看
	 */
	@Test
	public void findHDFS() throws Exception {
		//1.创建配置信息对象
		Configuration configuration = new Configuration();
		
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"victor");
		
		//2.获取查询路径下的文件状态信息
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		
		//3.遍历所有文件状态
		for (FileStatus fileStatus : listStatus) {
			if (fileStatus.isFile()) {
				System.out.println("f--"+fileStatus.getPath().getName());
			}else{
				System.out.println("d--"+fileStatus.getPath().getName());
			}
		}
		//4.关闭资源
		fs.close();
		
	}
	
	
	
	
	
	
	
	
}
