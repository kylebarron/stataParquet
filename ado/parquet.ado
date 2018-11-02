cap program drop sayHello
prog sayHello
    javacall ParquetReader sayHello, jar("/home/kyle/github/stata/stataParquet/target/stataParquet-0.1.0.jar")
end

