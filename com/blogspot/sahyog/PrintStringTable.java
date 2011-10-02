package com.sahyog;

import sun.jvm.hotspot.memory.StringTable;
import sun.jvm.hotspot.memory.SystemDictionary;
import sun.jvm.hotspot.oops.Instance;
import sun.jvm.hotspot.oops.InstanceKlass;
import sun.jvm.hotspot.oops.OopField;
import sun.jvm.hotspot.oops.TypeArray;
import sun.jvm.hotspot.runtime.VM;
import sun.jvm.hotspot.tools.Tool;
/**
 * Based on http://www.docjar.com/html/api/sun/jvm/hotspot/tools/PermStat.java.html 
 * @author puneet
 *
 */
public class PrintStringTable extends Tool {
	public PrintStringTable() {
		
	}
	class StringPrinter implements StringTable.StringVisitor {
		private OopField stringValueField;
		public StringPrinter() {
			VM vm = VM.getVM();
			SystemDictionary sysDict = vm.getSystemDictionary();
			InstanceKlass strKlass = sysDict.getStringKlass();
			stringValueField = (OopField) strKlass.findField("value", "[C");
		}
		@Override
		public void visit(Instance instance) {
			TypeArray charArray = ((TypeArray)stringValueField.getValue(instance));
			StringBuilder sb = new StringBuilder();
			for(long i=0;i<charArray.getLength();i++) {
				sb.append(charArray.getCharAt(i));
			}
			System.out.println(sb.toString());
		}
		
	}
	public static void main(String args[]) throws Exception {
		PrintStringTable pst = new PrintStringTable();
		pst.start(args);
		pst.stop();
	}

	@Override
	public void run() {
		StringTable table = VM.getVM().getStringTable();
		table.stringsDo(new StringPrinter());
	}
}
