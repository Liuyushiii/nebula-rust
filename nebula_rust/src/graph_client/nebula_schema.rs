use std::collections::HashMap;

/// contains all properties of both tag and edge
pub struct Tag{
    property_name: String,
    data_type: DataType,
    allow_null: bool,
    defaults: String,
    comment: String,
}

impl Tag {
    pub fn new(
        property_name: &str,
        data_type: DataType,
        allow_null: bool,
        defaults: &str,
        comment: &str, 
    ) -> Self{
        Tag { 
            property_name: property_name.to_string(), 
            data_type: data_type, 
            allow_null: allow_null, 
            defaults: defaults.to_string(), 
            comment: comment.to_string() }
    }
    pub fn to_string(&self)-> String{
        let mut line = "`".to_string();
        line += self.property_name.as_str();
        line += "` ";
        line += self.data_type.to_string().as_str();
        line += " ";
        if self.allow_null{
            line += "NULL ";
        }else{
            line += "NOT NULL ";
        }
        if self.defaults!="".to_string(){
            line += "DEFAULT \"";
            line += self.defaults.as_str();
            line += "\" ";
        }
        if self.comment!="".to_string(){
            line += "COMMENT \"";
            line += self.comment.as_str();
            line += "\" ";
        }
        line.to_string()
    }
}

/// frequently-used data type in NebulaGraph
pub enum DataType {
    Int,
    Bool,
    String,
    FixedString,
    Double,
    Int32,
    Int16,
    Int8,
    Float,
    Date,
    Time,
}

impl DataType {
    fn to_string(&self) -> String{
        match self{
            DataType::Int => String::from("int"),
            DataType::Bool => String::from("bool"),
            DataType::String => String::from("string"),
            DataType::FixedString => String::from("fixed_string"),
            DataType::Double => String::from("double"),
            DataType::Int32 => String::from("int32"),
            DataType::Int16 => String::from("int16"),
            DataType::Int8 => String::from("int8"),
            DataType::Float => String::from("float"),
            DataType::Date => String::from("date"),
            DataType::Time => String::from("time"),
        }
    }
}

/// tag or edge
pub enum ColType {
    Tag,
    Edge,
}

impl ColType {
    pub fn to_string(&self) -> String{
        match self{
            ColType::Tag => String::from("tag"),
            ColType::Edge => String::from("edge"),          
        }
    }
}

/// query of inserting tag
pub struct InsertTagQuery{
    pub space_name: String, 
    pub tag_name: String, 
    pub kv: HashMap<String, String>, 
    pub vid: String,
}
impl InsertTagQuery{
    pub fn new(
        space_name: String, 
        tag_name: String, 
        kv: HashMap<String, String>, 
        vid: String,
    ) -> Self{
        InsertTagQuery{
            space_name, 
            tag_name, 
            kv, 
            vid,
        }
    }
    pub fn to_string(&self)-> String{
        let mut query = String::from("use ");
        query += self.space_name.as_str();
        query += "; ";
        query += "INSERT VERTEX ";
        query += self.tag_name.as_str();
        query += " ";
        let mut keys = String::from("(");
        let mut values = String::from("(");
        for (k,v) in &self.kv{
            if keys.len()!=1{
                keys += ",";
                values += ",";
            }
            keys += k.as_str();
            values += v.as_str();
        }
        keys += ")";
        values += ")";
        query += keys.as_str();
        query += " VALUES \"";
        query += self.vid.as_str();
        query += "\":";
        query += values.as_str();
        query += ";";
        query
    }
}


/// query of inserting edge with rank
pub struct InsertEdgeQueryWithRank{
    pub space_name: String, 
    pub edge_name: String, 
    pub kv: HashMap<String, String>, 
    pub from_vertex: String, 
    pub to_vertex: String,
    pub rank: i64,
}
impl InsertEdgeQueryWithRank{
    pub fn new(
        space_name: String, 
        edge_name: String, 
        kv: HashMap<String, String>, 
        from_vertex: String, 
        to_vertex: String,
        rank: i64,
    ) -> Self{
        InsertEdgeQueryWithRank{
            space_name,
            edge_name, 
            kv, 
            from_vertex, 
            to_vertex,
            rank,
        }
    }
    pub fn to_string(&self)-> String{
        let mut query = String::from("use ");
        query += self.space_name.as_str();
        query += "; ";
        query += "INSERT EDGE IF NOT EXISTS ";
        query += self.edge_name.as_str();
        query += " ";
        let mut keys = String::from("(");
        let mut values = String::from("(");
        for (k,v) in &self.kv{
            if keys.len()!=1{
                keys += ",";
                values += ",";
            }
            keys += k.as_str();
            values += v.as_str();
        }
        keys += ")";
        values += ")";
        query += keys.as_str();
        query += " VALUES \"";
        query += self.from_vertex.as_str();
        query += "\" -> \"";
        query += self.to_vertex.as_str();
        query += "\"@";
        query += self.rank.to_string().as_str();
        query += ":";
        query += values.as_str();
        query += ";";
        query
    }
}

