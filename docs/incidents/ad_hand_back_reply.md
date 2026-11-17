# ad. `ad_hand_back_reply`

**What happened:** A coder ended its step with "I couldn't finish, please complete the remaining tasks manually". Nothing it lacked was a credential, access, a product decision, an ambiguous ticket or an approval; its words alone handed the work to a person.

**The guarantee:** A model reply that hands the work back for a reason not on the human-only list is not an ending: the implement pass tells the coder to change its approach and runs it again, and a reply that still changes nothing is a step without progress, which climbs the supervisor's ladder (one task, other tools, a re-split, the strongest model, then the stuck task deferred with its reason) until the run finishes with what is done. Only a reply naming something on the list makes a hand-back. Every coder's system prompt carries the same stance (`ownership::coder_stance`), and a source scan fails on a hand-off phrase in any prompt or message (`ownership`).
