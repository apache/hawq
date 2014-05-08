package com.pxf.tests.dataprepares.protobuf.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.pivotal.parot.fileformats.IDataPreparer;
import com.pivotal.parot.structures.tables.basic.Table;
import com.pxf.tests.dataprepares.protobuf.schema.PbGpProtos.People;
import com.pxf.tests.dataprepares.protobuf.schema.PbGpProtos.Person;

public class CustomProtobuffPreparer implements IDataPreparer {

	@Override
	public Object[] prepareData(int rows, Table dataTable) throws Exception {

		Object[] data = new Object[1];
		People.Builder peopleBuilder = People.newBuilder();

		for (int i = 0; i < rows; i++) {

			List<String> row = new ArrayList<String>();

			int id = i + 1;
			String name = new String("Name__" + id);
			String email = new String("name__" + id + "@url.com");

			List<Phone> phones = new ArrayList<Phone>();
			Phone phone = new Phone(new String("phone_number___" + id), new String("work"));
			phones.add(phone);

			row.add(name);
			row.add(String.valueOf(id));
			row.add(email);
			row.add("phone_number___" + id);
			row.add("2");

			dataTable.addRow(row);

			Person person = PromptForAddress(id, name, email, phones);
			peopleBuilder.addPerson(person);
		}

		data[0] = peopleBuilder.build();
		return data;
	}

	static Person PromptForAddress(int id, String name, String email, java.util.List<Phone> phones)
			throws IOException {

		Person.Builder personBuilder = Person.newBuilder();

		personBuilder.setId(id);
		personBuilder.setName(name);
		personBuilder.setEmail(email);

		for (Phone phone : phones) {
			Person.PhoneNumber.Builder phoneBuilder = Person.PhoneNumber.newBuilder()
					.setNumber(phone.num);

			String type = phone.type;
			if (type.equals("mobile")) {
				phoneBuilder.setType(Person.PhoneType.MOBILE);
			} else if (type.equals("home")) {
				phoneBuilder.setType(Person.PhoneType.HOME);
			} else {
				phoneBuilder.setType(Person.PhoneType.WORK);
			}
			personBuilder.addPhone(phoneBuilder);
		}

		return personBuilder.build();
	}
}
