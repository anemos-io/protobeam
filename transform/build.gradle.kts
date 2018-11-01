buildscript {
    //    repositories {
//        maven {
//            url "https://plugins.gradle.org/m2/"
//        }
//    }
    dependencies {
        classpath("net.ltgt.gradle:gradle-apt-plugin:0.13")
        classpath("com.google.protobuf:protobuf-gradle-plugin:0.8.6")
    }
}



plugins {
    // Apply the java-library plugin to add support for Java Library
    `java-library`
    idea
    id("com.google.protobuf") version "0.8.6"
}

dependencies {
    // This dependency is exported to consumers, that is to say found on their compile classpath.

    compile(project(":convert"))
    // This dependency is used internally, and not exposed to consumers on their own compile classpath.
    compile("org.apache.beam:beam-sdks-java-core:2.7.0")
    compile("org.apache.beam:beam-runners-direct-java:2.7.0")
    compile("org.apache.beam:beam-runners-google-cloud-dataflow-java:2.7.0")

    compile("org.apache.beam:beam-sdks-java-extensions-protobuf:2.7.0")
    compile("org.apache.beam:beam-sdks-java-extensions-google-cloud-platform-core:2.7.0")
    compile("org.apache.beam:beam-sdks-java-io-google-cloud-platform:2.7.0")
    compile("org.apache.beam:beam-sdks-java-io-common:2.7.0")

    // Use JUnit test framework
    testImplementation("junit:junit:4.12")
}

// In this section you declare where to find the dependencies of your project
repositories {
    // Use jcenter for resolving your dependencies.
    // You can declare any Maven/Ivy/file repository here.
    jcenter()
}

protobuf {

}