pluginManagement {
    repositories {
        mavenLocal()
        maven("https://plugins.gradle.org/m2/")
    }
}
rootProject.name = "proto-beam"

include("convert", "transform")

