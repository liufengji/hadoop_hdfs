package com.victor.hdfs.one;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class IOHDFS {

	// HDFS文件上传
	@Test
	public void putFileToHDFS() throws Exception {

		// 1.创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "victor");

		// 2.创建输入流
		FileInputStream inStream = new FileInputStream(new File("e:/hello.txt"));

		// 3.获取输出路径
		String pubFileName = "hdfs://hadoop102:9000/user/victor/hello.txt";
		Path writerPath = new Path(pubFileName);

		// 4.创建输出流
		FSDataOutputStream outStream = fs.create(writerPath);

		try {
			// 5 流对接
			IOUtils.copyBytes(inStream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(outStream);
			IOUtils.closeStream(inStream);
		}
	}

	/*
	 * HDFS文件下载
	 */
	@Test
	public void getFileToHDFS() throws Exception {

		// 1.创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "victor");

		// 2.获取读取文件路径
		String filename = "hdfs://hadoop102:9000/user/victor/hello.txt";

		// 3.创建读取path
		Path readPath = new Path(filename);

		// 4.创建输入流
		FSDataInputStream inStream = fs.open(readPath);

		try {
			// 5.流对接输出到控制台
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
		}
	}

	/*
	 * 定位文件读取 定位下载第一块
	 */
	@Test
	public void readFileSeek() throws Exception {

		// 1.创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI(""), configuration, "victor");

		// 2.获取输入流路径
		Path path = new Path("hdfs://hadoop102:9000/user/victor/tmp/hadoop-2.7.2.tar.gz");

		// 3.打开输入流
		FSDataInputStream fis = fs.open(path);

		// 4.创建输出流
		FileOutputStream fos = new FileOutputStream("e:/hadoop-2.7.2.tar.gz.part1");

		// 5.流对接
		byte[] b = new byte[1024];
		for (int i = 0; i < 1024 * 128; i++) {
			fis.read(b);
			fos.write(b);
		}

		// 6.关闭流
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);

	}

	/*
	 * 定位文件读取 下载第二块
	 */
	@Test
	public void readFileSeek2() throws Exception {

		// 1.创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000/"), configuration, "victor");

		// 2.获取输入流路径
		Path path = new Path("hdfs://hadoop102:9000/user/victor/tmp/hadoop-2.7.2.tar.gz");

		// 3.打开输入流
		FSDataInputStream fis = fs.open(path);

		// 4.创建输出流
		FileOutputStream fos = new FileOutputStream("e:/hadoop-2.7.2.tar.gz.part2");

		// 5.定位偏移量（第二块的首位）
		fis.seek(1024 * 128);

		// 6 流对接
		IOUtils.copyBytes(fis, fos, 1024);

		// 7.关闭流
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);

	}

	/*
	 * 一致性模型
	 */
	@Test
	public void writeFile() throws Exception {

		// 1创建配置信息对象
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "victor");

		// 2.创建文件输出流
		Path path = new Path("hdfs://hadoop102:9000/user/victor/hello.txt");
		FSDataOutputStream fos = fs.create(path);

		// 3.写数据
		fos.write("banzhangzhenshuai".getBytes());

		// 4.一致性刷新
		fos.hflush();

		// 5.关闭资源
		fos.close();

	}
}
