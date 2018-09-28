package com.victor.hdfs.two;

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

public class IOToHDFS {

	// 使用io流，文件上传
	@Test
	public void readfolderAtHDFS() throws Exception {

		// 1 .获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:8020"), configuration, "root");

		// 2.获取输出流
		FSDataOutputStream fos = fs.create(new Path("/user/victor/output/dongsi.txt"));

		// 3.创建输入流
		FileInputStream fis = new FileInputStream(new File("e:/dongsi.txt"));

		try {
			IOUtils.copyBytes(fis, fos, configuration);
		} catch (Exception e) {

		} finally {
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		}
	}

	// 使用io流，文件下载
	@Test
	public void getFileFromHDFS() throws Exception {

		// 1 .获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:8020"), configuration, "root");

		// 2.获取输入流
		FSDataInputStream fis = fs.open(new Path("/user/victor/output/dongsi.txt"));

		// 3.创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/dongsi.txt"));

		try {
			// 4.流的对接
			IOUtils.copyBytes(fis, fos, configuration);
		} catch (Exception e) {

		} finally {
			// 5.关闭资源
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		}

	}

	// 定位下载第1块数据
	@Test
	public void getFileFromHDFSSeek1() throws Exception {

		// 1 .获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:8020"), configuration, "root");

		// 2.获取输入流
		FSDataInputStream fis = fs.open(new Path("/user/victor/output/hadoop-2.7.2.tar.gz"));

		// 3.创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));

		// 4流对接（只读取128M）
		byte[] buf = new byte[1024];
		// 1024*1024*128

		for (int i = 0; i < 1024 * 128; i++) {
			fis.read(buf);
			fos.write(buf);
		}

		try {
			// 5.关闭资源
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		} catch (Exception e) {

		} finally {

		}

	}

	// 定位下载第2块数据
	@Test
	public void getFileFromHDFSSeek2() throws Exception {

		// 1 .获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:8020"), configuration, "root");

		// 2.获取输入流
		FSDataInputStream fis = fs.open(new Path("/user/victor/output/hadoop-2.7.2.tar.gz"));

		// 3.创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));

		// 4流的对接（指向第二块数据的首地址）
		// 定位到128M
		fis.seek(1024 * 1024 * 128);

		try {
			// 4.流的对接
			IOUtils.copyBytes(fis, fos, configuration);
		} catch (Exception e) {

		} finally {
			// 5.关闭资源
			IOUtils.closeStream(fis);
			IOUtils.closeStream(fos);
		}

	}
}
