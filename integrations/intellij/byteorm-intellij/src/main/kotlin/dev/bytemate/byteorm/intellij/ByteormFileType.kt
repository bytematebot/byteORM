package dev.bytemate.byteorm.intellij

import com.intellij.openapi.util.IconLoader
import com.intellij.openapi.fileTypes.LanguageFileType
import javax.swing.Icon

object ByteormFileType : LanguageFileType(ByteormLanguage) {
    override fun getName(): String = "ByteORM Schema"

    override fun getDescription(): String = "ByteORM schema file"

    override fun getDefaultExtension(): String = "bo"

    override fun getIcon(): Icon = IconLoader.getIcon("/icons/byteorm.svg", javaClass)
}
