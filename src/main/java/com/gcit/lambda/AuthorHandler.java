package com.gcit.lambda;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class AuthorHandler {
	
	private Connection getConnection() throws SQLException {

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		Connection conn = (Connection) DriverManager.getConnection(
				"jdbc:mysql://library.cy4p7z2rpnnf.us-east-1.rds.amazonaws.com:3306/library",
				"library",
				"library1");
		return conn;
	}
	
	public String getHandler(Integer id, Context context) throws SQLException {
		String sql = "SELECT * FROM tbl_author WHERE authorId=?";
		Author author = new Author();
		Connection conn = getConnection();
		System.out.println("Id received: " + id);
		System.out.println("Connection: " + conn);
		PreparedStatement stmt = conn.prepareStatement(sql);
		stmt.setInt(1, id);
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			author.setId(rs.getInt("authorId"));
			author.setName(rs.getString("authorName"));
		}
		conn.close();
		
		return author.toString();
	}
	
	public String createHandler(Author author, Context context) 
			throws SQLException, JsonParseException, JsonMappingException, IOException {
		
//		JsonNode authJson = (JsonNode)JsonNodeFactory.instance.objectNode();
//		ObjectMapper mapper = new ObjectMapper();
//		JsonNode authJson = mapper.readValue(authStr, JsonNode.class);
//		Author author = new Author();
//		author.setName(authJson.get("name").asText());
		
		String sql = "INSERT INTO tbl_author(authorName) VALUES(?)";
		Connection conn = getConnection();
		PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		stmt.setString(1, author.getName());
		stmt.executeUpdate();
		ResultSet rs = stmt.getGeneratedKeys();
		rs.next();
		author.setId(rs.getInt(1));
		conn.close();
		
		return author.toString();
	}

	public void updateHandler(Author author, Context context) 
			throws SQLException, JsonParseException, JsonMappingException, IOException {
		
		String sql = "UPDATE tbl_author SET authorName=? WHERE authorId=?";
		Connection conn = getConnection();
		PreparedStatement stmt = conn.prepareStatement(sql);
		stmt.setString(1, author.getName());
		stmt.setInt(2, author.getId());
		stmt.executeUpdate();
		conn.close();		
	}
	
	public void deleteHandler(Integer id, Context context) 
			throws SQLException, JsonParseException, JsonMappingException, IOException {
		
		String sql = "DELETE FROM tbl_author WHERE authorId=?";
		Connection conn = getConnection();
		PreparedStatement stmt = conn.prepareStatement(sql);
		stmt.setInt(1, id);
		stmt.executeUpdate();
		conn.close();	
	}

}
