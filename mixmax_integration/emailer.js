const path = require("path").resolve(__dirname + "/..");
require("dotenv").config({ path: path + "/.env" });
const MixmaxAPI = require("mixmax-api");
const jsforce = require("jsforce");
const readlineSync = require("readline-sync");

START_TIME = "06:00:00";
END_TIME = "10:00:00";

const currentDate = new Date();

startDate = new Date(currentDate.getTime());
startDate.setHours(START_TIME.split(":")[0]);
startDate.setMinutes(START_TIME.split(":")[1]);
startDate.setSeconds(START_TIME.split(":")[2]);

endDate = new Date(currentDate.getTime());
endDate.setHours(END_TIME.split(":")[0]);
endDate.setMinutes(END_TIME.split(":")[1]);
endDate.setSeconds(END_TIME.split(":")[2]);

SENIORITY_LEVEL = {};

SDR_OWNERS = {
	"Ellen Scott-Young": [
		process.env.ELLEN_YARONSINGER_API,
		process.env.ELLEN_YARONS_API,
	],
	"Katherine Hess": [process.env.KAT_YARON_S_API, process.env.KAT_YS_API],
	"Sophia Serseri": [
		process.env.SOPHIA_YSINGER_API,
		process.env.SOPHIA_Y_DASH_SINGER_API,
	],
	"Ryan Hutzley": [
		process.env.RYAN_YARON_DASH_SINGER_API,
		process.env.RYAN_YSING_API,
	],
	"Mark Levinson": [
		process.env.MARK_YARON_SINGER_API,
		process.env.MARK_Y_SINGER_API,
	],
};

const contacts = [];

const conn = new jsforce.Connection();
conn.login(
	process.env.USERNAME,
	process.env.PASSWORD + process.env.TOKEN,
	function (err, res) {
		if (err) {
			return console.error(err);
		}
		conn.query(
			`
        SELECT FirstName, Email, Account.Name, Account.SDR_Owner__c, Title, Account.Account_Score_Tier__c
        FROM CONTACT
        WHERE LastActivityDate != LAST_N_DAYS:60
        AND (EMAIL != null)
        AND (
            Job_Function__c = 'Data'
            OR Job_Function__c = 'AI'
            OR Job_Function__c = 'ML'
            OR Job_Function__c = 'Artificial Intelligence'
            OR Job_Function__c = 'Machine Learning'
            OR Job_Function__c = 'Data Science'
            OR Job_Function__c = 'Engineering'
        )
        AND (
            Management_Level__c = 'Director'
            OR Management_Level__c = 'Manager'
            OR Management_Level__c = 'Non-Manager'
        )
        AND (
            Contact_Status__c != 'Do Not Contact'
            OR Contact_Status__c != 'Responded - Circle Back'
        )
        AND AccountId != null
        AND Account.Priority_Account__c = FALSE
        AND Account.SDR_Owner__c != null
        AND Account.Account_Status__c
        NOT IN ('Bad Fit', 'Competitor', 'Active Opportunity', 'Closed Lost Opportunity', 'MLOps Company', 'Customer')
        `,
			function (err, res) {
				if (err) {
					return console.error(err);
				}
				console.log(res.totalSize);
				contacts.push(...res.records);
				if (!res.done) {
					getMore(res.nextRecordsUrl);
				} else {
					sortAndSend(contacts);
				}
			}
		);
	}
);

function getMore(nextRecordsUrl) {
	conn.queryMore(nextRecordsUrl, function (err, res) {
		if (err) {
			return console.error(err);
		}
		contacts.push(...res.records);
		if (!res.done) {
			getMore(res.nextRecordsUrl);
		} else {
			sortAndSend(contacts);
		}
	});
}

function sortAndSend(contacts) {
	while (true) {
		const res = readlineSync.question(
			`You are about to send emails. Do you wish to proceed? `
		);
		let regexYes = /y(?:es)?/i;
		let regexNo = /n(?:o)?/i;
		if (res.match(regexYes)) {
			break;
		} else if (res.match(regexNo)) {
			console.log("Process canceled");
			return;
		} else {
			console.log("Must be y/n");
		}
	}

	contacts.sort(function (a, b) {
		let newA = parseInt(a["Account"]["Account_Score_Tier__c"]?.slice(-1));
		let newB = parseInt(b["Account"]["Account_Score_Tier__c"]?.slice(-1));

		if (!isFinite(newA) && !isFinite(newB)) {
			return 0;
		}
		if (!isFinite(newA)) {
			return 1;
		}
		if (!isFinite(newB)) {
			return -1;
		}
		return newA - newB;
	});

	let formatted_contacts = contacts.map((contact) => {
		const newObj = {
			"SDR Owner": contact["Account"]["SDR_Owner__c"],
			email: contact["Email"],
			variables: {
				"SFDC Account: Account Name": contact["Account"]["Name"],
				"First Name": contact["FirstName"],
			},
		};
		return newObj;
	});

	for (const key in SDR_OWNERS) {
		const recipients = formatted_contacts.filter(
			(contact) => contact["SDR Owner"] === key
		);

		recipients.forEach((contact) => {
			delete contact["SDR Owner"];
			return contact;
		});

		if (recipients.length > 0 && recipients.length <= 45) {
			// console.log(`${key}: ${recipients.length}`);
			addUserToMixmax(recipients, SDR_OWNERS[key][0], key);
		} else if (recipients.length > 90 && SDR_OWNERS[key][1]) {
			// console.log(`${key}: ${recipients.length}`);
			addUserToMixmax(recipients.slice(0, 45), SDR_OWNERS[key][0], key);
			addUserToMixmax(recipients.slice(45, 90), SDR_OWNERS[key][1], key);
		} else if (recipients.length > 45 && SDR_OWNERS[key][1]) {
			// console.log(`${key}: ${recipients.length}`);
			addUserToMixmax(recipients.slice(0, 45), SDR_OWNERS[key][0], key);
			addUserToMixmax(recipients.slice(45), SDR_OWNERS[key][1], key);
		} else if (recipients.length > 45) {
			// console.log(`${key}: ${recipients.length}`);
			addUserToMixmax(recipients.slice(0, 45), SDR_OWNERS[key][0], key);
		}
	}
}

async function addUserToMixmax(recipients, API, key) {
	const api = new MixmaxAPI(API);
	const sequenceId = "6201c6e51fe985075676ae95";
	const sequence = api.sequences.sequence(sequenceId);
	const res = await sequence.addRecipients(recipients);
	const successes = res.filter((recipient) => recipient.status === "success");
	const errors = res.filter((recipient) => recipient.status === "error");
	const duplicates = res.filter(
		(recipient) => recipient.status === "duplicated"
	);
	key === "Katherine Hess" ? console.log(res) : null;
	console.log(
		`${key}: ${successes.length} successes, ${errors.length} errors, ${duplicates.length} duplicates`
	);
}
