/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ProyectSQL_Mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.swing.JOptionPane;
import java.sql.Statement;
import java.sql.ResultSet;

/**
 *
 * @author Jair Parra
 */
public class ConexionDB {

    private static Mongo mongo;
    private static DB db;
    private static DBCollection collection;
    private static GridFS gfsPhoto;
    private static GridFS gfsPhotoExtraer;

    static class Persona {

        public String primerapellido, segundoapellido, nombres, direccion, telefono1;
        public int nit;
        public Persona(int nit, String primerapellido, String segundoapellido, String nombres, String direccion, String telefono1) {
            this.nit = nit;
            this.primerapellido = primerapellido;
            this.segundoapellido = segundoapellido;
            this.nombres = nombres;
            this.direccion = direccion;
            this.telefono1 = telefono1;
        }
    }
    
    public static Connection GetConnection() {
        Connection conexion = null;
        int size = 0;
        byte[] fileBytes;
        try {
            System.out.println("Conexion Realizada con la base de datos SQL SERVER 2012 ");
            //Conexion con la base de datos de Sql server 
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            String url = "jdbc:sqlserver://PCJ;databaseName=prueba;user=sa;password=123456;";// 
            conexion = DriverManager.getConnection(url);
            Statement sentencia = conexion.createStatement();
            ResultSet rs = sentencia.executeQuery("SELECT nit,primerapellido,segundoapellido,nombres,direccion,telefono1 FROM dbo.nits order by 1");
            // Configuracion de conexion con mongodb
            mongo = new Mongo("localhost", 27017);
            db = mongo.getDB("Datos");
            DBCollection coleccion = db.getCollection("Persona");            
            while (rs.next()) {
                Persona p = new Persona(rs.getInt("nit"),rs.getString("primerapellido"),rs.getString("segundoapellido"),rs.getString("nombres"),rs.getString("direccion"),rs.getString("telefono1"));
                System.out.println(rs.getInt("nit"));
                BasicDBObject objeto = new BasicDBObject();         
                objeto.put("nit", p.nit);
                objeto.put("primerapellido", p.primerapellido);
                objeto.put("segundoapellido", p.segundoapellido);
                objeto.put("nombres", p.nombres);
                objeto.put("direccion", p.direccion);
                objeto.put("telefono1", p.telefono1);
                coleccion.insert(objeto);
                
                // extraer el documento de la base de datos y guarlo en directorio
                /*
                 System.out.println("extraer el documento de la base de datos y guarlo en directorio");
                 System.out.println(rs.getBigDecimal("docId") + " - " + rs.getString("nombre"));                
                 fileBytes = rs.getBytes(2);
                 OutputStream targetFile = new FileOutputStream("C://data//" + rs.getBigDecimal("docId") + "_" + rs.getString("nombre"));
                 targetFile.write(fileBytes);
                 targetFile.close();
                 */
                //ALMACENAR LA INFORMACION EN MONGODB
/*
                 System.out.println("ALMACENAR LA INFORMACION EN MONGODB");
                 dbFileName = rs.getBigDecimal("docId").toString();
                 imageFile = new File("C://data//" + rs.getBigDecimal("docId") + "_" + rs.getString("nombre"));                
                 GridFSInputFile gfsFile = gfsPhoto.createFile(imageFile);
                 gfsFile.setFilename(dbFileName);
                 gfsFile.save();
                 */
//                RECUPERAR LA INFORMACION DE MONGO Y GUARDARLA EN DIRECTORIO
                /*   
                 System.out.println("RECUPERAR LA INFORMACION DE MONGO Y GUARDARLA EN DIRECTORIO");                
                 GridFSDBFile iOSolicitud = gfsPhotoExtraer.findOne(rs.getBigDecimal("docId").toString());
                 iOSolicitud.writeTo("C://data//mongo//" + rs.getBigDecimal("docId") + "_Mongodb_" + rs.getString("nombre"));
                 size++;
                 */
            }
            conexion.close();
            mongo.close(); 
            System.out.println("Proceso Terminado Con Exito");
        } catch (ClassNotFoundException ex) {
            JOptionPane.showMessageDialog(null, ex, "Error1 en la Conexión con la BD " + ex.getMessage(), JOptionPane.ERROR_MESSAGE);
            conexion = null;
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, ex, "Error2 en la Conexión con la BD " + ex.getMessage(), JOptionPane.ERROR_MESSAGE);
            conexion = null;
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(null, ex, "Error3 en la Conexión con la BD " + ex.getMessage(), JOptionPane.ERROR_MESSAGE);
            conexion = null;
        } finally {
            return conexion;
        }
    }    
}