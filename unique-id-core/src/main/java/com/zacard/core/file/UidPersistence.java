package com.zacard.core.file;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * uid 持久化
 *
 * @author guoqw
 * @since 2018-04-20 15:52
 */
public class UidPersistence {

    /**
     * 默认文件大小：3G,可以持久化将近4亿id
     */
    private static final long MAX_FILE_SIZE_BYTE = 3 * 1024 * 1024 * 1024L;

    /**
     * 默认的文件保存路径
     */
    private static final String FILE_PATH = System.getProperty("user.home") + File.separator + "xuid";

    /**
     * 默认的文件名称
     */
    private static final String FILE_NAME = "someuid";

    private static final File XUID_FILE = new File(FILE_PATH + File.separator + FILE_NAME);

    /**
     * flush到文件的id数量,1page = 16kb = 16*1024/(8+1)
     * <p>
     * 8为一个long型，1位分隔符
     */
    private static final int EVERY_FLUSH_COUNT = 1820;

    /**
     * 拿到一个id先不直接写，优先缓存起来，达到一定量再写
     */
    private List<Long> uidWriteBuffer = new ArrayList<>(EVERY_FLUSH_COUNT);

    private int lineNumber = 1;

    public UidPersistence() {
        // 初始化文件
        initFile();
    }

    /**
     * 文件是否存满
     */
    public boolean isFull() {
        return XUID_FILE.length() >= MAX_FILE_SIZE_BYTE;
    }

    /**
     * 目前的方式：id用","分隔，每EVERY_FLUSH_COUNT个换行
     * <p>
     * <p>
     * TODO 改造成文件顺序读顺序写：
     * TODO 改成只写id，没有分隔符，用偏移量表示读取到的位置，读取后不删除数据。
     * TODO 写入的时候，结尾写入空白标识表示结尾，当写入达到文件上限的时候新建一个文件写，待当前文件读完后删除当前文件，偏移量设置为0即可
     * TODO 同时只允许存在2个文件
     * TODO see RandomAccessFile
     * TODO 考虑参考rockmq写文件的方式
     *
     * @param id uid
     * @return 写入文件后，如果文件满了返回false，否则返回true
     */
    public boolean writeToFile(long id) {
        if (id == 0) {
            return true;
        }
        if (isFull()) {
            return false;
        }
        uidWriteBuffer.add(id);
        if (uidWriteBuffer.size() < EVERY_FLUSH_COUNT) {
            return true;
        }
        // flush
        try (FileWriter writer = new FileWriter(XUID_FILE, true)) {
            writer.write(uidWriteBuffer.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(",")) + "\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return !isFull();
    }

    public boolean writeToFile(long[] ids) {
        if (ids.length == 0) {
            return true;
        }
        if (isFull()) {
            return false;
        }
        try (FileWriter writer = new FileWriter(XUID_FILE, true)) {
            StringBuilder line = new StringBuilder(2048);
            for (long id : ids) {
                line.append(",").append(id);
            }
            String lineStr = line.substring(1) + "\n";
            writer.write(lineStr);
/*            writer.write(LongStream.of(ids).boxed()
                    .map(Object::toString)
                    .collect(Collectors.joining(",")) + "\n");*/
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return !isFull();
    }

    /**
     * 从文件读取数据
     */
    public List<Long> readIds() {
        List<Long> result = new ArrayList<>(EVERY_FLUSH_COUNT);
        // 先查询写buffer是否有足够的(超过每次flush的半数)数据
        if (uidWriteBuffer.size() > (EVERY_FLUSH_COUNT >> 1)) {
            result.addAll(uidWriteBuffer);
            uidWriteBuffer.clear();
            return result;
        }
        // 读取一行的数据
        try {
            FileReader fileReader = new FileReader(XUID_FILE);
            BufferedReader reader = new BufferedReader(fileReader);
            String line;
            int currentLineNumber = lineNumber;
            while ((line = reader.readLine()) != null) {
                if (--currentLineNumber == 0) {
                    break;
                }
            }
            if (line != null) {
                String[] idArray = line.replace("\n", "").split(",");
                for (String id : idArray) {
                    try {
                        result.add(Long.parseLong(id));
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                }
            }
            lineNumber++;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private void initFile() {
        try {
            File path = new File(FILE_PATH);
            if (!path.exists()) {
                path.mkdirs();
            }
            if (!XUID_FILE.exists()) {
                XUID_FILE.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
