package dev.bytemate.byteorm.intellij

import com.intellij.extapi.psi.PsiFileBase
import com.intellij.openapi.fileTypes.FileType
import com.intellij.psi.FileViewProvider

class ByteormFile(viewProvider: FileViewProvider) : PsiFileBase(viewProvider, ByteormLanguage) {
    override fun getFileType(): FileType = ByteormFileType

    override fun toString(): String = "ByteORM Schema File"
}
