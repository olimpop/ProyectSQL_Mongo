/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ProyectSQL_Mongo;
import java.sql.Connection;
import javax.swing.JOptionPane;
/**
 *
 * @author Jair Parra
 */
public class MenuPrincipal {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        Connection miConexion;
        miConexion=ConexionDB.GetConnection();       
     
        if(miConexion!=null)
        {
            JOptionPane.showMessageDialog(null, "Proceso Terminado");            
        }
    }
    
}
