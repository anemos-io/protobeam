val repoUrl: String by project

plugins {
    id("net.ltgt.apt") version "0.20"
    id("com.google.protobuf") version "0.8.8"
    `java-library`
    `maven-publish`
    idea
}

dependencies {
    api("org.apache.commons:commons-math3:3.6.1")
    compile("org.apache.avro:avro:1.8.2")
    compile("com.google.protobuf:protobuf-java:3.6.1")
    implementation("com.google.protobuf:protobuf-java:3.6.1") //3.2.0
    implementation("com.google.protobuf:protobuf-java-util:3.6.1")
    implementation("com.google.cloud:google-cloud-bigquery:0.22.0-beta")
    testImplementation("junit:junit:4.12")
}

protobuf {

}

publishing {
    repositories {
        maven {
            // change to point to your repo, e.g. http://my.org/repo
            url = uri(repoUrl)
        }
    }
    publications {
        register("mavenJava", MavenPublication::class) {
            from(components["java"])
        }
    }
}