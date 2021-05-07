package org.example;


import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * 自定义classloader，
 * 1、解除双亲委派模式
 * 2、类加载时，优先使用本加载器查找，没找到再使用父加载器加载
 */
@Slf4j
public class JarVersionClassLoader extends URLClassLoader {

    private String dirPath;

    public JarVersionClassLoader(URL[] urls) {
        super(urls);
        log.info("init JarVersionClassLoader, URLs:{}", Arrays.toString(urls));
    }

    public JarVersionClassLoader(String dirPath) throws IOException {
        this(getJarUrls(dirPath));
        this.dirPath = dirPath;
    }

    public static URL[] getJarUrls(String dirPath) throws IOException {
        //todo
        String classPath = "D:\\IDEAWorkspace\\MyProject\\MyProject\\es6\\target\\es6";
        String os = System.getProperty("os.name");
        if (os.toLowerCase().contains("win") && classPath.startsWith("/")) {
            classPath = classPath.substring(1);
        }

        log.info("classPath: {}", classPath);
        List<URL> list = Files.list(Paths.get(classPath))
                              .filter(path -> path.toString().endsWith(".jar"))
                              .map(Path::toUri).flatMap(uri -> {
                    List<URL> urls = new ArrayList<>();
                    try {
                        urls.add(uri.toURL());
                    } catch (MalformedURLException e) {
                    }
                    return urls.stream();
                }).collect(Collectors.toList());
        URL[] urls = new URL[list.size()];
        list.toArray(urls);
        list.forEach(i -> System.out.println(i.toString()));
        return urls;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        //log.info("findClass:{}",name);
        Class<?> clazz = this.findLoadedClass(name);
        log.info("find exist findLoadedClass, name: {} , class: {}", name, clazz);
        if (clazz == null) {
            try {
                clazz = super.findClass(name);
                log.info("{}  loadClass: {}",clazz.getClassLoader().toString(), clazz);
            } catch (ClassNotFoundException e) {
                log.info("jarVersionClassLoader 找不到 clazz: {}", name);
            }
        }
        return clazz;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        //1、优先使用本加载器加载类
        Class<?> clazz = findClass(name);

        if (clazz == null) {
            //2、本加载器加载不成功，则调用父加载器加载
            ClassLoader classLoader = this;
            while (clazz == null && classLoader.getParent() != null) {
                classLoader = classLoader.getParent();
                clazz = classLoader.loadClass(name);
            }
            log.info("{} loadClass:{}", classLoader.toString(), name);
        }

        //3、如果子和父都加载不成功，抛出异常
        if (clazz == null) {
            log.error("JarVersionClassLoader loadClass:{} , result:fail.", name);
            throw new ClassNotFoundException(name);
        }

        return clazz;
    }
}
