package com.victor.hdfs.two;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class MyHDFS_T {

	// 上传文件
	@Test
	public void HDFSmain() throws Exception {
		// 0 设置参数 -DHADOOP_USER_NAME=root
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");

		// 2.获取文件系统
		FileSystem fs = FileSystem.get(configuration);

		// 3拷贝本地数据到集群
		fs.copyFromLocalFile(new Path("e:/xiyou.txt"), new Path("/xiyou.txt"));

		// 4.关闭fs
		fs.close();
	}

	// 上传文件
	@Test
	public void putFileToHDFS() throws Exception {

		// 1 创建配置信息对象
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

		// 2 要上传的文件所在的本地路径
		Path src = new Path("e:/jinlian.txt");

		// 3 要上传到hdfs的目标路径
		Path dst = new Path("hdfs://hadoop131:8020/jinlian.txt");

		// 4 拷贝文件
		fs.copyFromLocalFile(src, dst);

		fs.close();

	}

	// 获取文件系统
	@Test
	public void ConputFileToHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2.获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		// 3.打印文件系统
		System.out.println(fs.toString());
		// 4.关闭资源
		fs.close();
	}

	// 获取文件系统 通过core-site.xml 配置 方式 获取文件系统
	@Test
	public void ConputFileToHDFS1() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2.获取文件系统
		// FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),
		// configuration, "root");
		FileSystem fs = FileSystem.get(configuration);
		// 3.打印文件系统
		System.out.println(fs.toString());
		// 4.关闭资源
		fs.close();
	}

	// 上传文件 根据设置true或者false 设置 上传文件后本地文件是否删除，true是删除
	@Test
	public void putFileToHDFSd() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2.获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		// 3.执行上传文件命令
		fs.copyFromLocalFile(false, new Path("e:/bajie1.txt"), new Path("/user/victor/input"));
		// 4.关闭资源
		fs.close();
	}

	// 文件下载
	@Test
	public void getFiletoHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2.获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		// 3.执行上传文件命令
		// fs.copyToLocalFile(new Path("/user/victor/input/bajie1.txt"), new
		// Path("e:/bajie1.txt"));
		// fs.copyToLocalFile(false, new Path("/user/victor/input/bajie1.txt"),
		// new Path("e:/bajie1.txt"));
		fs.copyToLocalFile(false, new Path("/user/victor/input/bajie1.txt"), new Path("e:/bajie1.txt"), true);

		// 4.关闭资源
		fs.close();
	}

	// 创建目录
	@Test
	public void mkdirAtHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2.获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		// 3.执行创建文件夹操作 ,fs.mkdirs() 这个方法支持创建多级目录
		fs.mkdirs(new Path("/user/victor/output"));
		// 4.关闭资源
		fs.close();
	}

	// 删除文件夹
	@Test
	public void deleteAtHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2.获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
		// 3.执行创建文件夹操作 ,fs.mkdirs() 这个方法支持创建多级目录
		fs.delete(new Path("/hello1.txt"), true);
		// 4.关闭资源
		fs.close();
	}

	// 更改文件名称
	@Test
	public void renameAtHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2.获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

		// 3.执行创建文件夹操作 ,fs.mkdirs() 这个方法支持创建多级目录
		fs.rename(new Path("/xiyou.txt"), new Path("/ximenqing.txt"));
		// 4.关闭资源
		fs.close();
	}

	// 查看文件详情
	@Test
	public void readFileAtHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2.获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

		// 3.执行查看文件详情操作
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			// 文件名称
			System.out.println(status.getPath().getName());
			// 块的大小
			System.out.println(status.getBlockSize());
			// 文件内容的长度
			System.out.println(status.getLen());
			// 文件权限
			System.out.println(status.getPermission());
			System.out.println("-----------------");

			BlockLocation[] blockLocations = status.getBlockLocations();

			for (BlockLocation block : blockLocations) {
				System.out.println(block.getOffset());
				String[] hosts = block.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}

		}
		// 4.关闭资源
		fs.close();
	}

	// 查看文件详情
	@Test
	public void readfolderAtHDFS() throws Exception {
		// 1 创建配置信息对象
		Configuration configuration = new Configuration();
		// 2.获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

		//3.判断是文件夹还是文件
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		
		for(FileStatus status : listStatus){
			if(status.isFile()){
				System.out.println("f---"+status.getPath().getName()); 
			}else{
				System.out.println("d---"+status.getPath().getName()); 
			} 
		}
		// 4.关闭资源
		fs.close();
	}

}
