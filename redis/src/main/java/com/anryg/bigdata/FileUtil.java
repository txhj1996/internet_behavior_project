package com.anryg.bigdata;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/***
 * 文件操作工具类
 * @author linsh
 *
 */
public class FileUtil {
    /**
     * 读取整数配置数据
     * @param path
     * @param name
     * @return
     */
    public static int getPropertyValueInt(String path,String name) {
        int result = -1;
        result = Integer.valueOf(getPropertyValueString(path,name));
        return result;
    }
    /**
     * 读取字符型配置数据
     * @param path
     * @param name
     * @return
     */
    public static String getPropertyValueString(String path,String name) {
        String result = "";
        // 方法二：通过类加载目录getClassLoader()加载属性文件  
        InputStream in = FileUtil.class.getClassLoader()  
                .getResourceAsStream(path);  
        Properties prop = new Properties();  
        try {  
            prop.load(in);  
            result = prop.getProperty(name).trim();  
            System.out.println(name + ":" +  result);
        } catch (IOException e) {  
            System.out.println("读取配置文件出错");  
            e.printStackTrace();  
        } 
        return result;
    }
    /**
     * 读取布尔型配置数据
     * @param path
     * @param name
     * @return
     */
    public static boolean getPropertyValueBool(String path,String name) {
        boolean result = false;
        result = Boolean.parseBoolean(getPropertyValueString(path,name));
        return result;
    }
}