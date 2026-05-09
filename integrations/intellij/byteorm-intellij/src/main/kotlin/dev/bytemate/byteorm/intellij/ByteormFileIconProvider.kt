package dev.bytemate.byteorm.intellij

import com.intellij.ide.FileIconProvider
import com.intellij.openapi.project.Project
import com.intellij.openapi.vfs.VirtualFile
import javax.swing.Icon

class ByteormFileIconProvider : FileIconProvider {
    override fun getIcon(file: VirtualFile, flags: Int, project: Project?): Icon? {
        if (file.isDirectory) return null
        if (!file.extension.equals("bo", ignoreCase = true)) return null
        return ByteormFileType.icon
    }
}
