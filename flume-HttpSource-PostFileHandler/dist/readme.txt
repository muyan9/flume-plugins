该插件是自定义的httpsource的handler，用于接收multipart形式的post文件，处理文件上传

可接收多个文件同时上传

测试/模拟方法：
curl -F 'file=@a1.log' -F 'file=@a2.log' -F 'file=@a3.log' http://10.0.3.44:18889/

每个文件作为一个单独的event进入channel


**注意：由于event大小受参数