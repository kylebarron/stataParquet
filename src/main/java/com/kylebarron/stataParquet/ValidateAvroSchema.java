/* ValidateSchema.java
 *
 * Copyright June 2018 Tideworks Technology
 * Author: Roger D. Voss
 * MIT License
 */
package com.kylebarron.stataParquet;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static net.bytebuddy.matcher.ElementMatchers.*;

public final class ValidateAvroSchema {
  static void validate(final File schemaFile) throws IOException {
    final Schema avroSchema = new Schema.Parser().setValidate(true).parse(schemaFile);
    final List<String> fieldNames = avroSchema.getFields().stream()
            .map(field -> field.name().toUpperCase())
            .collect(Collectors.toList());
  }

  public static final class AvroSchemaInterceptor {
    public static String validateName(String name) {
      return name;
    }
  }

  public static void bytecodePatchAvroSchemaClass(final File avroSchemaClassesDir) throws ClassNotFoundException, IOException {
    final String avroSchemaClassPackageName = "org.apache.avro";
    final String avroSchemaFullClassName = avroSchemaClassPackageName + ".Schema";
    final String relativeFilePath = getClassRelativeFilePath(avroSchemaClassPackageName, avroSchemaFullClassName);
    final File clsFile = new File(avroSchemaClassesDir, relativeFilePath);
    Files.deleteIfExists(clsFile.toPath());
    final Class<?>[] methArgTypesMatch = { String.class };
    final DynamicType.Unloaded<?> avroSchemaClsUnloaded = new ByteBuddy()
            .rebase(Class.forName(avroSchemaFullClassName))
            .method(named("validateName").and(returns(String.class)).and(takesArguments(methArgTypesMatch))
                    .and(isPrivate()).and(isStatic()))
            .intercept(MethodDelegation.to(AvroSchemaInterceptor.class))
            .make();
    avroSchemaClsUnloaded.saveIn(avroSchemaClassesDir);
  }

  private static String getClassRelativeFilePath(final String pckgName, final String name) {
    final String dotClsStr = ".class";
    //noinspection StringBufferReplaceableByString
    return new StringBuilder(name.length() + dotClsStr.length())
                   .append(pckgName.replace('.', File.separatorChar)).append(File.separatorChar)
                   .append(name.substring(pckgName.length() + 1)).append(dotClsStr)
                   .toString();
  }
}