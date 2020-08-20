/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mydatabase_v8;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.ArrayList;

/**
 *
 * @author Russell Brown
 */
public class MyDatabase_v8 {
    
    public static int buffer_length = 1024;
    public static boolean test_move_up = false;
    public static boolean test_move_dn = false;
    public static boolean test_move_up_rec = false;
    public static boolean test_move_dn_rec = false;
    
    
    public static boolean database_exists(String database_name) throws Exception
    {
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   

        File f = new File(path);        

        boolean result = false;
        
        if (f.exists())
        {
            result = true;
        }
        
        
        return result;
        
        
    }
    
    public static void create_database(String database_name) throws Exception
    {        
        
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   

        File f = new File(path);        

        if (f.exists()) f.delete();        

        f.createNewFile();

        RandomAccessFile fl = new RandomAccessFile(path, "rw");

        fl.write("tables:\n".getBytes());

        fl.write("data:\n".getBytes());

        fl.close();
        
        
    }
    
    
    public static void delete_database(String database_name) throws Exception
    {        
        
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   

        File f = new File(path);        

        if (f.exists()) f.delete();        

        
        
        
    }
    
    
    public static void create_table(String database_name, String table_name, String[] column_names, String[] column_types) throws Exception
    {
        
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   
        
        RandomAccessFile f = new RandomAccessFile(path, "rw");
        
        // get the insert position for the table
        
        f.getChannel().position(0);
        while (true)
        {
            String line = f.readLine();
            if (line.indexOf("tables:") == 0) break;            
            if (f.getChannel().position() >= f.getChannel().size()) break;
        }
        
        boolean exists = false;
        long starting_position = 0;
        while (true)
        {
            starting_position = f.getChannel().position();
            String line = f.readLine();
            
            if (line.indexOf("data:") == 0)
            {
                break;
            }
            
            line = line.substring(0, line.indexOf(","));
                        
            if (line.contains(table_name))
            {
                exists = true;
                break;
            }
            
            if (line.indexOf("data:") == 0) break;            
            if (f.getChannel().position() >= f.getChannel().size()) break;
        }
        
        if (exists == false)
        {
            
            
             // format string
            
            String string = table_name + "," + column_names.length + ",";

            for (int i = 0; i < column_names.length; i++)
            {
                string += column_names[i].length() + ",";
                string += column_names[i] + ",";
            }

            for (int i = 0; i < column_types.length; i++)
            {
                string += column_types[i].length() + ",";
                string += column_types[i];
                
                if (i < column_types.length - 1) string += ",";
            }
            
            string += "\n";
            
            
            move_everything_down("create table", f, 0, starting_position, string, null);
        
            // write string
        
            f.getChannel().position(starting_position);            
            f.write(string.getBytes());

        }
        
        
        
        f.close();
        
        
    }
    
    
    
    public static ArrayList[] get_table_info(String database_name, String table_name) throws Exception
    {
        ArrayList column_names = new ArrayList();
        ArrayList column_types = new ArrayList();
        
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   
        
        RandomAccessFile f = new RandomAccessFile(path, "rw");
                
        f.getChannel().position(0);
        
        boolean look_at_tables = false;
        
        while (true)
        {
            String line = f.readLine();
            
            if (line.indexOf("data:") == 0)
            {
                look_at_tables = false;
                break;
            }
            
            if (look_at_tables == true)
            {
                String[] array2 = line.split(",");
                
                if (array2[0].equals(table_name))
                {
                   
                    {
                        int cols = Integer.valueOf(array2[1]);
                        
                        int ci = 0;
                        
                        int start = 0;
                        
                        for (int i = 0; i < line.length(); i++)
                        {
                            
                            if (line.charAt(i) == ',')
                            {
                                ci++;
                                
                                if (ci == 2)
                                {
                                    start = i+1;
                                    break;
                                }
                                
                            } 
                            
                        }
                        
                        // column names
                        
                        int c = start;
                        ci = start;
                        
                        for (int i = 0; i < cols; i++)
                        {

                            String length = "";

                            while (true)
                            {
                                if (line.charAt(ci) == ',')
                                {
                                    break;
                                }

                                length += line.charAt(ci);

                                c++;
                                ci++;
                                if (ci >= line.length()) break;
                            }

                            int p1 = ci + 1;

                            
                            ci += Integer.valueOf(length) + 1;
                        
                            String value = line.substring(p1, ci);
                                                        
                            column_names.add(new Object[]{length, value});
                            
                            ci++;
                        
                        }
                        
                        
                        // column types
                        
                        for (int i = 0; i < cols; i++)
                        {

                            String length = "";

                            while (true)
                            {
                                if (line.charAt(ci) == ',')
                                {
                                    break;
                                }

                                length += line.charAt(ci);

                                c++;
                                ci++;
                                if (ci >= line.length()) break;
                            }
                            
                            int p1 = ci + 1;

                            
                            ci += Integer.valueOf(length) + 1;
                        
                            String value = line.substring(p1, ci);
                                                        
                            column_types.add(new Object[]{length, value});
                            
                            ci++;
                        }
                        
                        
                        
                    }
                    
                }
                
            }
            
            
            if (line.indexOf("tables:") == 0)
            {
                look_at_tables = true;
            }
            
            
        }
        
       // System.out.println(list.size() + "  " + list2.size());
        
        f.close();
        
        return new ArrayList[]{column_names, column_types};
    }
    
    public static int get_number_of_columns(String database_name, String table_name) throws Exception
    {
         
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   
        
        RandomAccessFile f = new RandomAccessFile(path, "rw");
        
        
        f.getChannel().position(0);
        while (true)
        {
            String line = f.readLine();
            if (line.indexOf("tables:") == 0) break;            
            if (f.getChannel().position() >= f.getChannel().size()) break;
        }
        
        String table_string = "";
        while (true)
        {
            table_string = f.readLine();
            if (table_string.indexOf(table_name) == 0)
            {
                break;
            }
            
            if (table_string.indexOf("data:") == 0) break;            
            if (f.getChannel().position() >= f.getChannel().size()) break;
        }
        
        
        String[] table_array = table_string.split(",");
        int cols = Integer.valueOf(table_array[1]);
        
        f.close();
        
        return cols;
        
    }
    
    
    
    
    //-------------------------
    
    
    public static boolean move_everything_up(String operation, RandomAccessFile f, long starting_position, long ending_position) throws Exception
    {
        // move everything up
        
        boolean result = true;
           
            long next_write_pos = starting_position;
            long next_read_pos = ending_position;
            
            int length = buffer_length;
            boolean end_of_file = false;
            
            int n = 0;
            
            while (true)
            {
                // from top to bottom

                f.getChannel().position(next_read_pos);
                
                if (f.getChannel().position() + length > f.getChannel().size())
                {
                    length = (int)(f.getChannel().size() - f.getChannel().position());
                    end_of_file = true;
                }

                byte[] data = new byte[length];

                f.read(data);
                                
                next_read_pos = f.getChannel().position();
                
                
                f.getChannel().position(f.getChannel().size());
                
                // save the block to the end of the file for recovery
                
                long length2 = f.getChannel().size();
                
                long starting_pos = f.getChannel().position();
                
                try
                {
                    if (length > 0)
                    {
                
                        f.write((operation + ":" + length + ":" + next_write_pos + ":" + next_read_pos + "\n").getBytes());

                        f.write(data);    

                        if (data[data.length - 1] != '\n')
                        {
                            f.write("\n".getBytes());                        
                        }
                        
                        
                        f.write(("recovery point" + ":" + starting_pos + "\n").getBytes());

                    }
                    else
                    {
                        break;
                    }


                }
                catch (Exception ex)
                {
                    result = false;
                    ex.printStackTrace();
                }

                
                //--
                
                // write the data
                
                if (result == true)
                {
                    try
                    {


                        f.getChannel().position(next_write_pos);             

                        f.write(data);
                        
                        

                        
                    }
                    catch (Exception ex)
                    {
                        System.out.println("error");
                        result = false;
                        ex.printStackTrace();
                        System.exit(0);
                    }
                }
                
                //-
                
                
                if (test_move_up == true)
                {                
//                    System.out.println(n);
                    if (n == 5)
                    {
                        result = false;
                        break;
                    }
                }
                
                
                if (result == true)
                {

                    f.setLength(length2);

                    if (end_of_file == true) break;

                    next_write_pos = f.getChannel().position();
            
                }
                else
                {
                    break;
                }
                
                n++;
                
                
            }
            
            if (result == true)
            {
                f.setLength(f.getChannel().position());
            }
            
            return result;
        
    }
    
    public static boolean move_everything_up(String operation, RandomAccessFile f, long starting_position, long ending_position, String line) throws Exception
    {
        
        
        // move everything up
        
        boolean result = true;
           
            long next_write_pos = starting_position;
            long next_read_pos = ending_position;
            
            int length = buffer_length;
            boolean end_of_file = false;
            
            int n = 0;
                        
            
            
            while (true)
            {
                // from top to bottom

                f.getChannel().position(next_read_pos);
                
                if (f.getChannel().position() + length > f.getChannel().size())
                {
                    length = (int)(f.getChannel().size() - f.getChannel().position());
                    end_of_file = true;
                }

                byte[] data = new byte[length];

                f.read(data);
                
//                for (int i = 0; i < data.length; i++)
//                {
//                    char cc = (char)data[i];
//                    System.out.print(cc);
//                }
                                
                next_read_pos = f.getChannel().position();
                
                
                f.getChannel().position(f.getChannel().size());
                
                // save the block to the end of the file for recovery
                
                long length2 = f.getChannel().size();
                
                long starting_pos = f.getChannel().position();
                
                try
                {
                    if (length > 0)
                    {
                
                        f.write((operation + ":" + length + ":" + next_write_pos + ":" + next_read_pos + ":" + starting_position + ":" + line + "\n").getBytes());

                        f.write(data);    

                        if (data[data.length - 1] != '\n')
                        {
                            f.write("\n".getBytes());                        
                        }
                        
                        
                        f.write(("recovery point" + ":" + starting_pos + "\n").getBytes());

                    }
                    else
                    {
                        break;
                    }


                }
                catch (Exception ex)
                {
                    result = false;
                    ex.printStackTrace();
                }

                
                //--
                
                // write the data
                
                if (result == true)
                {
                    try
                    {


                        f.getChannel().position(next_write_pos);  
                        
                        f.write(data);
                        
                        
                    }
                    catch (Exception ex)
                    {
                        result = false;
                        ex.printStackTrace();
                    }
                }
                
                //-
                
                if (test_move_up == true)
                {                
                    if (n == 0)
                    {
                        result = false;
                        break;
                    }
                }
                
                
                if (result == true)
                {

                    f.setLength(length2);

                    if (end_of_file == true) break;

                    next_write_pos = f.getChannel().position();
            
                }
                else
                {
                    break;
                }
                
                n++;
                
                
            }
            
            
            if (result == true)
            {
                f.setLength(f.getChannel().position());
            }
            
            return result;
        
        
    }
    
    
    
    public static boolean move_everything_down(String operation, RandomAccessFile f, long position, long target_position, String string, String string2) throws Exception
    {
        boolean result = true;
        
            // move everything down
                        
                    
            int length = buffer_length;
                        
            boolean end_of_data = false;
            
            long saved_length = 0;
            
            
            if (position == 0)
            {

                f.getChannel().position(f.getChannel().size());


                if (f.getChannel().position() - length < target_position)
                {
                    length = (int)(f.getChannel().position() - target_position);
                    end_of_data = true;
                }


                position = f.getChannel().position() - length;

            }
            
            int n = 0;
            
            while (true)
            {

                byte[] data = new byte[length];

              long next_write_pos = position;
              
                f.getChannel().position(position);
                f.read(data);
                
                
                // save the data to the end of the file for recovery



                try
                {

                    if (length > 0)
                    {

                        String blanks = "";
                        for (int i = 0; i < string.length()-1; i++)
                        {
                            blanks += "_";
                        }
                        blanks += "\n";

                        f.write(blanks.getBytes()); 
                        
//                        
//                        if (test_move_dn == true)
//                        {
//                            if (n == 0)
//                            {
//                                System.exit(0);
//                            }
//                        }

                        f.getChannel().position(f.getChannel().size());


                        saved_length = f.getChannel().position();


                        long starting_pos = f.getChannel().position();
                        
                        String str = string.substring(0, string.length() - 1);

                        f.write((operation + ":" + length + ":" + next_write_pos + ":" + target_position + ":" + str + ":" + string2).getBytes());

//                        if (test_move_dn == true)
//                        {
//                            if (n == 0)
//                            {
//                                System.exit(0);
//                            }
//                        }
                        
                        
                        
                        f.write(data);

//                        if (test_move_dn == true)
//                        {
//                            if (n == 0)
//                            {
//                                System.exit(0);
//                            }
//                        }

                        if (data[data.length - 1] != '\n')
                        {
                            f.write("\n".getBytes());                        
                        }


                        f.write(("recovery point" + ":" + starting_pos + "\n").getBytes());

                        
                        if (test_move_dn == true)
                        {
                            if (n == 1)
                            {
                                result = false;
                                break;                                
                            }
                        }

                    }
                    else
                    {
                        break;
                    }



                }
                catch (Exception ex)
                {
                    ex.printStackTrace();
                }
                  
                
                if (test_move_dn == true)
                {
//                    System.out.println(n);
//                    if (n >= 2)
//                    {
//                        System.out.println("a");
//                        break;
//                    }
                }
                
                
                
                // write the data
                
                try
                {
                    

                    f.getChannel().position(position + string.length());
                    
                    f.write(data);
                    
                    
                    f.setLength(saved_length);
              
                
                }
                catch (Exception ex)
                {
                    result = false;
                    ex.printStackTrace();
                }
                
                
                if (result == true)
                {

                    if (end_of_data == true) break;


                    if (position - length < target_position)
                    {                 
                        length = (int)(position - target_position);
                        position = target_position;
                        end_of_data = true;
                    }
                    else
                    {            
                        position -= length;

                    }
                }
                else
                {
                    break;
                }
                
                
                n++;

            }
            
            
            return result;
    }
    
    
    
    
    
    
    //-------------------------

    
    
    
    public static void get_beginning_of_line(RandomAccessFile f) throws Exception
    {
                        
        long position = f.getChannel().position();
        
        if (position > 0)
        {

            while (true)
            {
                f.getChannel().position(position);

                char c = (char)f.read();

                if (c == '\n') break;
                
                position--;
                
                if (position <= 0) break;

            }

            if (position > 0)
            {
                f.getChannel().position(position+1);
            }
            else
            {
                f.getChannel().position(position);
            }
            
        }
        
    }
    
    
    
    
    
    
    public static boolean IsDouble(String string)
    {
        boolean result = true;
        
        
        try
        {
            Double.parseDouble(string);            
        }
        catch (Exception ex)
        {
            result = false;
        }
        
        return result;
    }
    
    
    
    public static boolean IsDateTime(String string)
    {
        boolean result = true;
        
        
        try
        {
            Long.parseLong(string);            
        }
        catch (Exception ex)
        {
            result = false;
        }
        
        return result;
    }
    
    
    
    public static boolean IsInteger(String string)
    {
        boolean result = true;
        
        
        try
        {
            Integer.parseInt(string);            
        }
        catch (Exception ex)
        {
            result = false;
        }
        
        return result;
    }
    
    
    
    public static boolean IsString(String string)
    {
        boolean result = true;
        
        
        return result;
    }
    
    
    
    //-------------------------
    
    
    public static boolean check_input(String database_name, String table_name, String[] array) throws Exception
    {
        ArrayList[] arr = get_table_info(database_name, table_name);
        
        ArrayList column_types = arr[1];
        
        boolean result = true;
        
        int cols = get_number_of_columns(database_name, table_name);
        
        
        if (array.length != cols)
        {         
            result = false;
        }
        else
        {

            
            for (int i = 0; i < column_types.size(); i++)
            {
                Object[] oarr = (Object[])column_types.get(i);

                
                if (oarr[1].equals("datetime"))
                {
                    if (IsDateTime(array[i]) == false)
                    {
                        result = false;
                        break;
                    }
                }
                else if (oarr[1].equals("string"))
                {
                    if (IsString(array[i]) == false)
                    {
                        result = false;
                        break;
                    }
                }
                else if (oarr[1].equals("int"))
                {
                    if (IsInteger(array[i]) == false)
                    {
                        result = false;
                        break;
                    }
                }
                else if (oarr[1].equals("double"))
                {
                    if (IsDouble(array[i]) == false)
                    {
                        result = false;
                        break;
                    }
                }


            }
        
        }
        
        
        
        return result;
        
    }
    
    
    
    public static void insert(String database_name, String table_name, String[] array) throws Exception
    {
        try
        {
            
            if (check_input(database_name, table_name, array) == true)
            {
                


                String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   


                RandomAccessFile f = new RandomAccessFile(path, "rw");

                FileLock lock = f.getChannel().lock();
                
                if (lock != null)
                {

                    // get the starting size
                    f.getChannel().position(0);
                    while (true)
                    {
                        String line = f.readLine();
                        if (line.indexOf("data:") == 0) break;            
                        if (f.getChannel().position() >= f.getChannel().size()) break;
                    }

                    long size = f.getChannel().position();

                    // check that table exists

                    f.getChannel().position(0);
                    while (true)
                    {
                        String line = f.readLine();
                        if (line.indexOf("tables:") == 0) break;            
                        if (f.getChannel().position() >= f.getChannel().size()) break;
                    }

                    boolean exists = false;
                    String table_string = "";
                    while (true)
                    {
                        table_string = f.readLine();
                        if (table_string.contains(table_name))
                        {
                            exists = true;
                            break;
                        }

                        if (table_string.indexOf("data:") == 0) break;            
                        if (f.getChannel().position() >= f.getChannel().size()) break;
                    }

                    if (exists == true)
                    {
                        int record_number = 0;

                        if (f.getChannel().size() > size)
                        {
                                // get the next record number
                            f.getChannel().position(f.getChannel().size()-2);
                            get_beginning_of_line(f);
                            String line = f.readLine();
                            String[] line_array = line.split(",");
                            record_number = Integer.valueOf(line_array[0])+1;
                        }

                        // format insert string

                        String line = record_number + "," + table_name + ",";
                        for (int i = 0; i < array.length; i++)
                        {
                            line += array[i].length();
                            line += ",";
                        }
                        for (int i = 0; i < array.length; i++)
                        {
                            line += array[i];
                        }



                        line += "\n";

                        // write the string


                        f.getChannel().position(f.getChannel().size());

                        long position = f.getChannel().position();
                        long position2 = position + line.length();


                        f.write(line.getBytes());

                        boolean error = false;


                        if (f.getChannel().size() < position2)
                        {
                            error = true;
                        }

                        if (error == false)
                        {            

                            f.getChannel().position(position);

                            String string = "";

                            while (true)
                            {
                                string += (char)f.read();

                                if (f.getChannel().position() >= position2) break;
                            }

                            if (line.equals(string) == false)
                            {
                                error = true;
                            }

                        }


                        if (error)
                        {
                            f.setLength(position); // roll back the file & remove the garbage data
                            throw new Exception("error during insert");
                        }




                    }
                    
                    lock.close();

                }

                f.close();
                
                
                
            
            }
            else
            {
                throw new Exception("unable to accept insert");
            }
            
            
        
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        
    }
    
    
    
    
    public static byte[] select_data(String database_name, int record_number, int col_id) throws Exception
    {
        
        String[] array = null;
        
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   
        
        byte[] result = null;
                        
        RandomAccessFile f = new RandomAccessFile(path, "rw");
        
        FileLock lock = f.getChannel().lock();
                
        if (lock != null)
        {
        
            // get the starting position for the data
            f.getChannel().position(0);
            while (true)
            {
                String line = f.readLine();
                if (line.indexOf("data:") == 0) break;            
                if (f.getChannel().position() >= f.getChannel().size()) break;
            }
            
            int count = 0;
            
            String table_name = "";
            boolean add_table_name = false;
            String rec_number = "";
            boolean add_rec_number = true;
            
            long beginning_of_data = f.getChannel().position();
            
            
            
            while (true)
            {
                char c = (char)f.read();
                if (c == ',')
                {
                    count++;
                    if (count == 1)
                    {
                        add_rec_number = false;
                        add_table_name = true;
                    }
                    else if (count == 2)
                    {
                        long saved_position = f.getChannel().position();
                        
                        f.getChannel().position(0);
                        while (true)
                        {
                            String line = f.readLine();
                            if (line.indexOf("tables:") == 0) break;            
                            if (f.getChannel().position() >= f.getChannel().size()) break;
                        }

                        String table_string = "";
                        while (true)
                        {
                            table_string = f.readLine();
                            if (table_string.indexOf(table_name) == 0)
                            {
                                break;
                            }

                            if (table_string.indexOf("data:") == 0) break;            
                            if (f.getChannel().position() >= f.getChannel().size()) break;
                        }


                        String[] table_array = table_string.split(",");
                        int cols = Integer.valueOf(table_array[1]);
        
                        // get the total length
                        f.getChannel().position(saved_position);
                        
                        int total_length = 0;
                        ArrayList col_sizes = new ArrayList();
                        
                        String string = "";
                        count = 0;
                        long start_of_data = 0;
                        while (true)
                        {
                            c = (char)f.read();
                            if (c == ',')
                            {
                                total_length += Integer.valueOf(string);
                                col_sizes.add(Integer.valueOf(string));
                                string = "";
                                count++;
                                if (count >= cols) 
                                {
                                    start_of_data = f.getChannel().position();
                                    break;
                                }
                            }
                            else
                            {
                                string += c;
                            }
                        }
                        
                       
                        f.getChannel().position(f.getChannel().position() + total_length+1);
                        
                        if (f.getChannel().position() >= f.getChannel().size()) break;
                        
                        int recn = Integer.valueOf(rec_number);
                        
                        if (recn == record_number)
                        {
                            
                            
                            f.getChannel().position(start_of_data);

                            for (int i = 0; i < col_sizes.size(); i++)
                            {
                                int col_length = (int)col_sizes.get(col_id);
                                byte[] data = new byte[col_length];                                
                                f.read(data);
                                
                                result = data;
                                
                                if (i == col_id) break;
                                
                                f.getChannel().position(f.getChannel().position() - 1);
                                
//                                for (int i2 = 0; i2 < data.length; i2++)
//                                {
//                                    char c2 = (char)data[i2];
//                                    System.out.print(c2);
//                                }
//                                System.out.print("  ");

                            }


                            
                            break;
                        }
                        
                        rec_number = "";
                        table_name = "";
                        add_rec_number = true;
                        add_table_name = true;
                        count = 0;
                        beginning_of_data = f.getChannel().position();
//                        
                    }
                }
                else
                {
                    if (add_rec_number == true)
                    {
                        rec_number += c;
                    }
                    else if (add_table_name == true)
                    {
                        table_name += c;
                    }
                    
                }
                
            }


            
        
            lock.close();
        
        }
        
        f.close();
        
        return result;
        
        
    }
    

    public static byte[] select_table_name(String database_name, int record_number) throws Exception
    {
        
        String[] array = null;
        
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   
        
        byte[] result = null;
                        
        RandomAccessFile f = new RandomAccessFile(path, "rw");
        
        FileLock lock = f.getChannel().lock();
                
        if (lock != null)
        {
        
            // get the starting position for the data
            f.getChannel().position(0);
            while (true)
            {
                String line = f.readLine();
                if (line.indexOf("data:") == 0) break;            
                if (f.getChannel().position() >= f.getChannel().size()) break;
            }
            
            int count = 0;
            
            String table_name = "";
            boolean add_table_name = false;
            String rec_number = "";
            boolean add_rec_number = true;
            
            long beginning_of_data = f.getChannel().position();
            
            
            
            while (true)
            {
                char c = (char)f.read();
                if (c == ',')
                {
                    count++;
                    if (count == 1)
                    {
                        add_rec_number = false;
                        add_table_name = true;
                    }
                    else if (count == 2)
                    {
                        long saved_position = f.getChannel().position();
                        
                        f.getChannel().position(0);
                        while (true)
                        {
                            String line = f.readLine();
                            if (line.indexOf("tables:") == 0) break;            
                            if (f.getChannel().position() >= f.getChannel().size()) break;
                        }

                        String table_string = "";
                        while (true)
                        {
                            table_string = f.readLine();
                            if (table_string.indexOf(table_name) == 0)
                            {
                                break;
                            }

                            if (table_string.indexOf("data:") == 0) break;            
                            if (f.getChannel().position() >= f.getChannel().size()) break;
                        }


                        String[] table_array = table_string.split(",");
                        int cols = Integer.valueOf(table_array[1]);
        
                        // get the total length
                        f.getChannel().position(saved_position);
                        
                        int total_length = 0;
                        ArrayList col_sizes = new ArrayList();
                        
                        String string = "";
                        count = 0;
                        long start_of_data = 0;
                        while (true)
                        {
                            c = (char)f.read();
                            if (c == ',')
                            {
                                total_length += Integer.valueOf(string);
                                col_sizes.add(Integer.valueOf(string));
                                string = "";
                                count++;
                                if (count >= cols) 
                                {
                                    start_of_data = f.getChannel().position();
                                    break;
                                }
                            }
                            else
                            {
                                string += c;
                            }
                        }
                        
                       
                        f.getChannel().position(f.getChannel().position() + total_length+1);
                        
                        if (f.getChannel().position() >= f.getChannel().size()) break;
                        
                        int recn = Integer.valueOf(rec_number);
                        
                        if (recn == record_number)
                        {
                            result = table_name.getBytes();


                            
                            break;
                        }
                        
                        rec_number = "";
                        table_name = "";
                        add_rec_number = true;
                        add_table_name = true;
                        count = 0;
                        beginning_of_data = f.getChannel().position();
//                        
                    }
                }
                else
                {
                    if (add_rec_number == true)
                    {
                        rec_number += c;
                    }
                    else if (add_table_name == true)
                    {
                        table_name += c;
                    }
                    
                }
                
            }


            
        
            lock.close();
        
        }
        
        f.close();
        
        return result;
        
        
    }
    
    
    public static byte[] update(String database_name, int record_number, String[] array2) throws Exception
    {
        
        
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   
        
        byte[] result = null;
                        
        RandomAccessFile f = new RandomAccessFile(path, "rw");
        
        FileLock lock = f.getChannel().lock();
                
        if (lock != null)
        {
        
            // get the starting position for the data
            f.getChannel().position(0);
            while (true)
            {
                String line = f.readLine();
                if (line.indexOf("data:") == 0) break;            
                if (f.getChannel().position() >= f.getChannel().size()) break;
            }
            
            int count = 0;
            
            String table_name = "";
            boolean add_table_name = false;
            String rec_number = "";
            boolean add_rec_number = true;
            
            
            long line_position = 0;
            
            while (true)
            {
                char c = (char)f.read();
                if (c == ',')
                {
                    count++;
                    if (count == 1)
                    {
                        add_rec_number = false;
                        add_table_name = true;
                    }
                    else if (count == 2)
                    {
                        long saved_position = f.getChannel().position();
                        
                        f.getChannel().position(0);
                        while (true)
                        {
                            String line = f.readLine();
                            if (line.indexOf("tables:") == 0) break;            
                            if (f.getChannel().position() >= f.getChannel().size()) break;
                        }

                        String table_string = "";
                        while (true)
                        {
                            table_string = f.readLine();
                            if (table_string.indexOf(table_name) == 0)
                            {
                                break;
                            }

                            if (table_string.indexOf("data:") == 0) break;            
                            if (f.getChannel().position() >= f.getChannel().size()) break;
                        }


                        String[] table_array = table_string.split(",");
                        int cols = Integer.valueOf(table_array[1]);
        
                        // get the total length
                        f.getChannel().position(saved_position);
                        
                        int total_length = 0;
                        
                        String string = "";
                        count = 0;
                        while (true)
                        {
                            c = (char)f.read();
                            if (c == ',')
                            {
                                total_length += Integer.valueOf(string);
                                string = "";
                                count++;
                                if (count >= cols) 
                                {
                                    break;
                                }
                            }
                            else
                            {
                                string += c;
                            }
                        }
                        
                        int recn = Integer.valueOf(rec_number);
                        
                        if (recn == record_number)
                        {
                            //......
                            
                            boolean error = false;

                            long old_length = f.getChannel().size();
                            long position = line_position;
                            long position2 = f.getChannel().position() + total_length;
                            long new_length = old_length - (position2 - position)-1;//*******


                            String original_line = "";
                            f.getChannel().position(position);
                            while (true)
                            {
                                char c2 = (char)f.read();
                                original_line += c2;
                                if (f.getChannel().position() >= position2) break;
                            }
                            
                            

                           

                            boolean result2 = move_everything_up("update-move_everything_up", f, line_position, position2+1, original_line);
                            
                            
                            
                            if (result2 == true)
                            {
                                error = false;
                            }
                            

                            if (f.getChannel().size() != new_length)
                            {
//                                System.out.println(f.getChannel().size() + "  " + new_length);
                                error = true;
                            }
                            

                            if (error == false)
                            {

                                // format insert string

                                String line = "";
                                line += rec_number + "," + table_name + ",";
                                for (int i2 = 0; i2 < array2.length; i2++)
                                {
                                    line += array2[i2].length();
                                    line += ",";
                                }
                                for (int i2 = 0; i2 < array2.length; i2++)
                                {
                                    line += array2[i2];
                                }

                                
                                position2 = line_position + line.length()+1;

                                line += "\n";

                                original_line += "\n";

                                
                                result2 = move_everything_down("update-move_everything_down2", f, 0, line_position, line, original_line);
                                                             
//                                f.getChannel().position(line_position);
//                                f.write("*".getBytes());
                                
//                                System.exit(0);


                                
                                if (result2 == false)
                                {
                                    error = true;
                                    break;
                                }



                                // write the string
                                f.getChannel().position(line_position);

//                                line += "\n";

    
//                                System.out.print(line);

                                f.write(line.getBytes());
                                

//                                System.exit(0);

                                /*
                                if (f.getChannel().size() < position2)
                                {
                                    error = true;
                                }

                                if (error == false)
                                {            
                                    
                                    // check the contents of the line

                                    f.getChannel().position(position);

                                    string = "";

                                    while (true)
                                    {
                                        string += (char)f.read();

                                        if (f.getChannel().position() >= position2) break;
                                    }
                                                            
//                                    System.out.print(line + "  " + string);

//                                    line = line.substring(0, line.length() - 1);
                                    if (line.equals(string) == false)
                                    {
                                        
                                        error = true;
                                    }

                                }

                                */



                            }

//                            if (test_move_up == false && test_move_dn == false)
//                            {
//
//                                if (error)
//                                {
//                                    throw new Exception("error during update");
//                                }
//                            
//                            }
                            
                            
                            
                            //......

                            
                            break;
                        }
                       
                        f.getChannel().position(f.getChannel().position() + total_length+1);
                        
                        if (f.getChannel().position() >= f.getChannel().size()) break;
                        
                        line_position = f.getChannel().position();
                        
                        
                        rec_number = "";
                        table_name = "";
                        add_rec_number = true;
                        add_table_name = true;
                        count = 0;
//                        
                    }
                }
                else
                {
                    if (add_rec_number == true)
                    {
                        rec_number += c;
                    }
                    else if (add_table_name == true)
                    {
                        table_name += c;
                    }
                    
                }
                
            }


            
        
            lock.close();
        
        }
        
        f.close();
        
        return result;
        
        
    }
    
    
    
    
    
    public static byte[] delete(String database_name, int record_number) throws Exception
    {
        
        
        String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   
        
        byte[] result = null;
                        
        RandomAccessFile f = new RandomAccessFile(path, "rw");
        
        FileLock lock = f.getChannel().lock();
                
        if (lock != null)
        {
        
            // get the starting position for the data
            f.getChannel().position(0);
            while (true)
            {
                String line = f.readLine();
                if (line.indexOf("data:") == 0) break;            
                if (f.getChannel().position() >= f.getChannel().size()) break;
            }
            
            int count = 0;
            
            String table_name = "";
            boolean add_table_name = false;
            String rec_number = "";
            boolean add_rec_number = true;
            
            
            long line_position = 0;
            
            while (true)
            {
                char c = (char)f.read();
                if (c == ',')
                {
                    count++;
                    if (count == 1)
                    {
                        add_rec_number = false;
                        add_table_name = true;
                    }
                    else if (count == 2)
                    {
                        long saved_position = f.getChannel().position();
                        
                        f.getChannel().position(0);
                        while (true)
                        {
                            String line = f.readLine();
                            if (line.indexOf("tables:") == 0) break;            
                            if (f.getChannel().position() >= f.getChannel().size()) break;
                        }

                        String table_string = "";
                        while (true)
                        {
                            table_string = f.readLine();
                            if (table_string.indexOf(table_name) == 0)
                            {
                                break;
                            }

                            if (table_string.indexOf("data:") == 0) break;            
                            if (f.getChannel().position() >= f.getChannel().size()) break;
                        }


                        String[] table_array = table_string.split(",");
                        int cols = Integer.valueOf(table_array[1]);
        
                        // get the total length
                        f.getChannel().position(saved_position);
                        
                        int total_length = 0;
                        
                        String string = "";
                        count = 0;
                        while (true)
                        {
                            c = (char)f.read();
                            if (c == ',')
                            {
                                total_length += Integer.valueOf(string);
                                string = "";
                                count++;
                                if (count >= cols) 
                                {
                                    break;
                                }
                            }
                            else
                            {
                                string += c;
                            }
                        }
                        
                        int recn = Integer.valueOf(rec_number);
                        
                        if (recn == record_number)
                        {
                            //......
                            
                            boolean error = false;

                            long old_length = f.getChannel().size();
                            long position = line_position;
                            long position2 = f.getChannel().position() + total_length;
                            long new_length = old_length - (position2 - position)-1;//*******


                            String original_line = "";
                            f.getChannel().position(position);
                            while (true)
                            {
                                char c2 = (char)f.read();
                                original_line += c2;
                                if (f.getChannel().position() >= position2) break;
                            }
                            
                            

                           

                            boolean result2 = move_everything_up("delete-move_everything_up", f, line_position, position2+1, original_line);
                            
                            
                            /*
                            if (result2 == true)
                            {
                                error = false;
                            }
                            

                            if (f.getChannel().size() != new_length)
                            {
//                                System.out.println(f.getChannel().size() + "  " + new_length);
                                error = true;
                            }
                            

                            if (error == false)
                            {

                                // format insert string

                                String line = "";
                                line += rec_number + "," + table_name + ",";
                                for (int i2 = 0; i2 < array2.length; i2++)
                                {
                                    line += array2[i2].length();
                                    line += ",";
                                }
                                for (int i2 = 0; i2 < array2.length; i2++)
                                {
                                    line += array2[i2];
                                }

                                
                                position2 = line_position + line.length()+1;

                                line += "\n";

                                original_line += "\n";

                                
                                result2 = move_everything_down("update-move_everything_down2", f, 0, line_position, line, original_line);
                                                             
//                                f.getChannel().position(line_position);
//                                f.write("*".getBytes());
                                
//                                System.exit(0);


                                
                                if (result2 == false)
                                {
                                    error = true;
                                    break;
                                }



                                // write the string
                                f.getChannel().position(line_position);

//                                line += "\n";

    
//                                System.out.print(line);

                                f.write(line.getBytes());
                                

//                                System.exit(0);

                                
                                if (f.getChannel().size() < position2)
                                {
                                    error = true;
                                }

                                if (error == false)
                                {            
                                    
                                    // check the contents of the line

                                    f.getChannel().position(position);

                                    string = "";

                                    while (true)
                                    {
                                        string += (char)f.read();

                                        if (f.getChannel().position() >= position2) break;
                                    }
                                                            
//                                    System.out.print(line + "  " + string);

//                                    line = line.substring(0, line.length() - 1);
                                    if (line.equals(string) == false)
                                    {
                                        
                                        error = true;
                                    }

                                }

                                



                            }
                            */

//                            if (test_move_up == false && test_move_dn == false)
//                            {
//
//                                if (error)
//                                {
//                                    throw new Exception("error during update");
//                                }
//                            
//                            }
                            
                            
                            
                            //......

                            
                            break;
                        }
                       
                        f.getChannel().position(f.getChannel().position() + total_length+1);
                        
                        if (f.getChannel().position() >= f.getChannel().size()) break;
                        
                        line_position = f.getChannel().position();
                        
                        
                        rec_number = "";
                        table_name = "";
                        add_rec_number = true;
                        add_table_name = true;
                        count = 0;
//                        
                    }
                }
                else
                {
                    if (add_rec_number == true)
                    {
                        rec_number += c;
                    }
                    else if (add_table_name == true)
                    {
                        table_name += c;
                    }
                    
                }
                
            }


            
        
            lock.close();
        
        }
        
        f.close();
        
        return result;
        
        
    }
    
    
    
    
    
    public static void perform_recovery(String database_name) throws Exception
    {
        try
        {
            
            String path = System.getProperty("user.dir") + "\\" + database_name + ".dat";   
        
            File fl = new File(path);       
            
            if (fl.exists())
            {                
                RandomAccessFile f = new RandomAccessFile(path, "rw");
                
                // go to the end of the file
                f.getChannel().position(f.getChannel().size() - 2);
                
                get_beginning_of_line(f);
                
                String line = f.readLine();
                
                String[] arr = line.split(":");
                                
                // look to see if we have a recover point at the end of the file
                if (arr[0].equals("recovery point"))
                {
                
                    long recovery_point = Long.valueOf(arr[1]);

                    f.getChannel().position(recovery_point);

                    line = f.readLine();                
                    arr = line.split(":");

                    
                    if (arr[0].equals("delete-move_everything_up"))
                    {
                        
                       int length = Integer.valueOf(arr[1]);
                        
                        if (length > 0)
                        {                            
                         //   System.out.println("testing recovery");
                            long next_write_pos = Long.valueOf(arr[2]);
                            long next_read_pos = Long.valueOf(arr[3]);
                            long target_position = Long.valueOf(arr[4])+1;
                            String line_data = arr[5];
                            
                            line_data += "\n";
                            
                            byte[] data = new byte[length];                
                            f.read(data);
                            

                            f.getChannel().position(next_write_pos);
                            f.write(data);
                            
                            next_write_pos = f.getChannel().position();
                            
                            f.getChannel().position(next_read_pos);
                            
                            f.setLength(recovery_point);
                            

                            boolean result = move_everything_up("delete-move_everything_up", f, next_write_pos, next_read_pos, line_data);
                            
                            
                          
                            
                            
                            
                        }
                        
                    }
                    
                    else if (arr[0].equals("update-move_everything_up"))
                    {
                        int length = Integer.valueOf(arr[1]);
                        
                        if (length > 0)
                        {                            
                            
                            long next_write_pos = Long.valueOf(arr[2]);
                            long next_read_pos = Long.valueOf(arr[3]);
                            long target_position = Long.valueOf(arr[4]);
                            String line_data = arr[5];
                            
                            line_data += "\n";//*****
                            
                            byte[] data = new byte[length];                
                            f.read(data);
                            

                            f.getChannel().position(next_write_pos);
                            f.write(data);
                            
                            next_write_pos = f.getChannel().position();
                            
                            f.getChannel().position(next_read_pos);
                            
                            f.setLength(recovery_point);
                            
                            boolean result = move_everything_up("update-move_everything_up", f, next_write_pos, next_read_pos, line_data);
                            
                              
                            if (result == true)
                            {
                                 boolean result2 = move_everything_down("update-move_everything_down2", f, 0, target_position, line_data, null);

                                if (result2 == true)
                                {

                                    f.getChannel().position(target_position);

                                    f.write(line_data.getBytes());
                                
                                }
                                
                                
                            }
                            
                            
                            
                        }
                    }
                    else if (arr[0].equals("update-move_everything_down2"))
                    {
                        int length = Integer.valueOf(arr[1]);
                        
                        if (length > 0)
                        {
                            
                            long position = Long.valueOf(arr[2]);
                            long target_position = Long.valueOf(arr[3]);
                            String line_data = arr[4];
                            String original_line = arr[5];
                            
                           
                            
                           long end_position = position + length;
                           long end_position2 = end_position + line_data.length()+1;
//                           long diff = end_position2 - end_position;
                           
                           f.getChannel().position(end_position2);
                           byte[] data = new byte[length];
                           f.read(data);
                           
                            f.getChannel().position(end_position);
                            f.write(data);
                            f.setLength(end_position + data.length);
                            
//                            System.out.println("down3");
                           
                           
                            original_line += "\n";
                           
                           move_everything_down(arr[0], f, 0, target_position, original_line, null); 
                            
                           
                            f.getChannel().position(target_position);
                            
                            
                             f.write(original_line.getBytes());
                            
                        }
                    }
                
                
                }
                
                
                
                
                f.close();
                
            }
            
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }
    
    
    
    
    //----------------------------
    
    // test create & insert
    public static void test1() throws Exception
    {
        if (database_exists("mydatabase") == false)
        {
            create_database("mydatabase");
            
            create_table("mydatabase", "customers", new String[]{"first name", "last name"}, new String[]{"string", "string"});   
        
        }
        
        for (int i = 0; i < 10; i++)
        {
            
            insert("mydatabase", "customers", new String[]{"john" + String.valueOf(i), "smith" + String.valueOf(i)});
        
        }

    }
    
    // test select
    public static void test2() throws Exception
    {
        
        
        byte[] data = select_table_name("mydatabase", 4);
        
        if (data != null)
        {
            for (int i = 0; i < data.length; i++)
            {
                char c = (char)data[i];
                System.out.print(c);
            }
        }
        
    }
    
    // test update
    public static void test3() throws Exception
    {
        
        String[] arr = new String[]{"aahgkjgkjgjkgkju", "bb9954"};
        
        update("mydatabase", 5, arr);
        
        
    }
    
    // test delete
    public static void test4() throws Exception
    {        
        delete("mydatabase", 5);
    }
    
    
    // test recovery on update
     public static void test5() throws Exception
    {
        
        if (database_exists("mydatabase") == true)
        {
            delete_database("mydatabase");
        }
        
        if (database_exists("mydatabase") == false)
        {
            create_database("mydatabase");
            
            create_table("mydatabase", "customers", new String[]{"first name", "last name"}, new String[]{"string", "string"});   
        
        }
        
        for (int i = 0; i < 200; i++)
        {
            
            insert("mydatabase", "customers", new String[]{"john" + String.valueOf(i), "smith" + String.valueOf(i)});
        
        }
        



        
        String[] arr = new String[]{"aahgkjgkjgjkgkju", "bb9954hello world good morning everyone"};
                
        
        
//        test_move_up = true; 
//        test_move_dn = true;
        update("mydatabase", 50, arr);
        
//        test_move_up = false;
//        test_move_dn = false;
//        perform_recovery("mydatabase");
        
        
        
        

    }
     
     
    
    // test recovery on delete
     public static void test6() throws Exception
    {
        
        if (database_exists("mydatabase") == true)
        {
            delete_database("mydatabase");
        }
        
        if (database_exists("mydatabase") == false)
        {
            create_database("mydatabase");
            
            create_table("mydatabase", "customers", new String[]{"first name", "last name"}, new String[]{"string", "string"});   
        
        }
        
        for (int i = 0; i < 200; i++)
        {
            
            insert("mydatabase", "customers", new String[]{"john" + String.valueOf(i), "smith" + String.valueOf(i)});
        
        }
        
        
        
//        test_move_up = true; 
//        test_move_dn = true;
        delete("mydatabase", 50);
        
        test_move_up = false;
        test_move_dn = false;
        perform_recovery("mydatabase");
        
        
        
        

    }
     
     
     
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        
        
      test6();
      
      
        
        
        
    }
    
}

