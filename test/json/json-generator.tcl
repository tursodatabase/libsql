#!/usr/bin/tclsh
#
# Generate SQL that will populate an SQLite database with about 100 megabytes
# of pseudo-random JSON text.
#
#     tclsh json-generator.tcl | sqlite3 json110mb.db
#
# srand() is used to initialize the random seed so that the same JSON
# is generated for every run.
#
expr srand(12345678)
set wordlist {
   ability   able      abroad    access    account   act
   action    active    actor     add       address   adept
   adroit    advance   advice    affect    age       ageless
   agency    agent     agile     agree     air       airfare
   airline   airport   alert     almond    alpha     always
   amend     amount    amplify   analyst   anchor    angel
   angelic   angle     ankle     annual    answer    antique
   anybody   anyhow    appeal    apple     apricot   apt
   area      argon     arm       army      arrival   arsenic
   art       artful    article   arugula   aside     ask
   aspect    assist    assume    atom      atone     attempt
   author    autumn    average   avocado   award     awl
   azure     back      bacon     bag       bagel     bake
   baker     balance   ball      balloon   bamboo    banana
   band      banjo     bank      barium    base      basil
   basin     basis     basket    bass      bat       bath
   battery   beach     beak      bean      bear      bearcub
   beauty    beef      beet      beige     being     bell
   belly     belt      bench     bend      benefit   best
   beta      better    beyond    bicycle   bid       big
   bike      bill      bird      biscuit   bismuth   bisque
   bit       black     blank     blest     blind     bliss
   block     bloom     blue      board     boat      body
   bokchoy   bone      bonus     book      bookish   boot
   border    boron     boss      bossy     bottle    bottom
   bow       bowl      bowtie    box       brain     brainy
   branch    brave     bravely   bread     break     breath
   breezy    brick     bridge    brie      brief     briefly
   bright    broad     broil     bromine   bronze    brother
   brow      brown     brush     buddy     budget    buffalo
   bug       bugle     bull      bunch     burger    burly
   burrito   bus       busy      butter    button    buy
   buyer     byte      cab       cabbage   cabinet   cable
   cadet     cadmium   caesium   cake      calcium   caliper
   call      caller    calm      calmly    camera    camp
   can       canary    cancel    candle    candy     cap
   capable   caper     capital   captain   car       carbon
   card      care      career    careful   carp      carpet
   carrot    carry     case      cash      cassava   casual
   cat       catch     catfish   catsear   catsup    cause
   cave      celery    cell      century   chain     chair
   chalk     chance    change    channel   chapter   chard
   charge    charity   chart     check     cheddar   cheery
   cheese    chicken   chicory   chiffon   child     chin
   chip      chives    choice    chowder   chum      church
   circle    city      claim     clam      class     classic
   classy    clay      clean     cleaner   clear     clearly
   clerk     click     client    climate   clock     clorine
   closet    clothes   cloud     clown     club      clue
   cluster   coach     coast     coat      cobbler   cobolt
   cod       code      coffee    colby     cold      collar
   college   comb      combine   comet     comfort   command
   comment   common    company   complex   concept   concern
   concert   conduit   consist   contact   contest   context
   control   convert   cook      cookie    copilot   copper
   copy      coral     cordial   corn      corner    corny
   correct   cost      count     counter   country   county
   couple    courage   course    court     cover     cow
   cowbird   crab      crack     craft     crash     crazy
   cream     credit    creek     cress     crevice   crew
   crimson   croaker   crop      cross     crowd     cube
   cuckoo    cuisine   culture   cup       current   curve
   cut       cyan      cycle     dagger    daily     dance
   dare      darter    data      date      day       daylily
   deal      dear      dearly    debate    debit     decade
   decimal   deep      deft      deftly    degree    delay
   deluxe    deposit   depth     design    desk      detail
   device    dew       diamond   diet      dig       dill
   dinner    dip       direct    dirt      dish      disk
   display   diver     divide    divine    doctor    dodger
   donut     door      dot       double    dough     draft
   drag      dragon    drama     draw      drawer    drawing
   dream     drill     drink     drive     driver    drop
   drum      dry       dryer     drywall   duck      due
   dump      dusk      dust      duty      dye       eagle
   ear       earring   earth     ease      east      easy
   eat       economy   edge      editor    eel       effect
   effort    egg       eight     elbow     elegant   element
   elf       elk       email     emerald   employ    end
   endive    endless   energy    engine    enjoy     enter
   entry     equal     equip     error     escape    essay
   eternal   evening   event     exam      example   excuse
   exit      expert    extent    extreme   eye       face
   fact      factor    factual   fail      failure   fair
   fajita    fall      family    fan       fang      farm
   farmer    fat       fault     feature   feed      feel
   feeling   fench     fennel    festive   few       fiber
   field     fig       figure    file      fill      film
   filter    final     finance   finding   finger    finish
   fire      fish      fishing   fit       fitting   five
   fix       flier     flight    floor     floral    florine
   flour     flow      flower    fly       flying    focus
   fold      folding   food      foot      force     forest
   forever   forgive   form      formal    format    fortune
   forum     frame     free      freedom   freely    fresh
   friend    frog      front     fruit     fuchsia   fuel
   fun       funny     future    gain      galaxy    gallium
   game      gamma     gap       garage    garden    garlic
   gas       gate      gather    gauge     gear      gem
   gene      general   gentle    gently    gherkin   ghost
   gift      give      glad      glass     gleeful   glossy
   glove     glue      goal      goat      goby      gold
   goldeye   golf      good      gouda     goulash   gourd
   grab      grace     grade     gram      grand     grape
   grapes    grass     gravy     gray      great     green
   grits     grocery   ground    group     grouper   grout
   growth    guard     guave     guess     guest     guide
   guitar    gumbo     guppy     habit     hacksaw   haddock
   hafnium   hagfish   hair      half      halibut   hall
   hammer    hand      handle    handy     hanger    happy
   hat       havarti   hay       haybale   head      health
   healthy   hearing   heart     hearty    heat      heavy
   heel      height    helium    hello     help      helpful
   herald    herring   hide      high      highly    highway
   hill      hip       hipster   hire      history   hit
   hoki      hold      hole      holiday   holly     home
   honest    honey     hook      hope      hopeful   horizon
   horn      horse     host      hotel     hour      house
   housing   human     humane    humor     hunt      hurry
   ice       icecube   icefish   icy       idea      ideal
   image     impact    impress   inch      income    indigo
   initial   inkpen    insect    inside    intense   invite
   iodine    iridium   iron      island    issue     item
   ivory     jacket    jargon    javelin   jello     jelly
   jewel     job       jocund    join      joint     joke
   jovial    joy       joyful    joyous    judge     juice
   jump      junior    jury      just      justice   kale
   keel      keep      kelp      ketchup   key       keyhole
   keyway    khaki     kick      kid       kidney    kiloohm
   kind      kindly    king      kitchen   kite      kiwi
   knee      knife     krill     krypton   kumquat   lab
   lace      lack      ladder    lake      lamp      lamprey
   land      laser     laugh     law       lawn      lawyer
   layer     lead      leader    leading   leaf      leafy
   league    leather   leave     lecture   leek      leg
   lemon     length    lentil    lesson    let       letter
   lettuce   level     library   life      lift      light
   lily      lime      limit     line      linen     link
   lip       list      listen    lithium   lively    living
   lizard    load      loan      lobster   local     lock
   log       long      longfin   look      lotus     love
   lovely    loving    low       lucid     luck      luffa
   lunch     lung      machine   magenta   magnet    mail
   main      major     make      mall      manager   mango
   manner    many      map       march     market    maroon
   martian   master    match     math      matter    maximum
   maybe     meal      meaning   meat      media     medium
   meet      meeting   melody    melon     member    memory
   mention   menu      mercury   merry     mess      message
   messy     metal     meter     method    micron    middle
   might     mile      milk      mind      mine      minimum
   minnow    minor     mint      minute    mirror    miss
   mission   misty     mix       mixer     mixture   mobile
   mode      model     moment    monitor   monk      month
   moon      moray     morning   most      motor     mouse
   mouth     move      mover     movie     much      mud
   mudfish   muffin    mullet    munster   muon      muscle
   music     mustard   nail      name      nation    native
   natural   nature    navy      neat      neatly    nebula
   neck      needle    neon      nerve     net       network
   neutron   news      nibble    nice      nickel    night
   niobium   nobody    noise     noodle    normal    north
   nose      note      nothing   notice    nova      novel
   number    nurse     nursery   oar       object    offer
   office    officer   oil       okay      okra      old
   olive     one       onion     open      opening   opinion
   option    orange    orbit     orchid    order     oregano
   other     ounce     outcome   outside   oven      owner
   oxygen    oyster    pace      pack      package   page
   pager     paint     pair      pale      pan       pancake
   papaya    paper     pardon    parent    park      parking
   parsley   parsnip   part      partner   party     pass
   passage   past      pasta     path      patient   pattern
   pause     pay       pea       peace     peach     peacock
   peahen    peak      peanut    pear      pearl     pen
   penalty   pencil    pension   people    pepper    perch
   perfect   period    permit    person    phase     phone
   photo     phrase    physics   piano     pick      picture
   pie       piece     pigeon    pike      pilot     pin
   pink      pinkie    pious     pipe      pitch     pizza
   place     plan      plane     planet    plant     planter
   plastic   plate     play      player    playful   plenty
   pliers    plum      pod       poem      poet      poetry
   point     police    policy    pollock   pony      pool
   pop       popover   poptart   pork      port      portal
   post      pot       potato    pound     powder    power
   present   press     price     pride     primary   print
   prior     private   prize     problem   process   produce
   product   profile   profit    program   project   promise
   prompt    proof     proper    protein   proton    public
   puff      puffer    pull      pumpkin   pup       pupfish
   pure      purple    purpose   push      put       quality
   quark     quarter   quiet     quill     quit      quote
   rabbit    raccoon   race      radiant   radio     radish
   radium    radon     rain      rainbow   raise     ramp
   ranch     range     rasp      rate      ratio     ray
   razor     reach     read      reading   real      reality
   reason    recipe    record    recover   red       redeem
   reed      reef      refuse    region    regret    regular
   relaxed   release   relief    relish    remote    remove
   rent      repair    repeat    reply     report    request
   reserve   resist    resolve   resort    rest      result
   return    reveal    review    reward    ribbon    rice
   rich      ride      ridge     right     ring      rise
   risk      river     rivet     road      roast     rock
   rocket    role      roll      roof      room      rope
   rose      rough     roughy    round     row       royal
   rub       ruby      rudder    ruin      rule      run
   runner    rush      rust      sacred    saddle    safe
   safety    sail      salad     salami    sale      salmon
   salt      sample    sand      sander    sandy     sauce
   save      saving    saw       scale     scampi    scene
   scheme    school    score     screen    script    sea
   search    season    seat      second    secret    sector
   seemly    self      sell      senate    senior    sense
   series    serve     set       shake     shape     share
   shark     shell     shift     shine     shiny     ship
   shock     shoe      shoot     shop      shovel    show
   side      sign      signal    silk      silly     silver
   simple    sing      singer    single    sink      site
   size      skill     skin      sky       slate     sleep
   sleepy    slice     slide     slip      smart     smell
   smelt     smile     smoke     smooth    snap      snipe
   snow      snowy     sock      socket    sodium    soft
   softly    soil      sole      solid     song      sorrel
   sort      soul      sound     soup      source    south
   space     spare     speech    speed     spell     spend
   sphere    spice     spider    spirit    spite     split
   spoon     sport     spot      spray     spread    spring
   squab     square    squash    stable    staff     stage
   stand     staple    star      start     state     status
   stay      steak     steel     step      stern     stew
   stick     still     stock     stone     stop      store
   storm     story     strain    street    stress    strike
   string    stroke    strong    studio    study     stuff
   style     sugar     suit      sulfur    summer    sun
   sunny     sunset    super     superb    surf      survey
   sweet     swim      swing     switch    symbol    system
   table     tackle    tail      tale      talk      tan
   tank      tap       tape      target    task      taste
   tau       tea       teach     teal      team      tear
   tell      ten       tender    tennis    tent      term
   test      tetra     text      thanks    theme     theory
   thing     think     thread    throat    thumb     ticket
   tidy      tie       tiger     till      time      timely
   tin       tip       title     toast     today     toe
   tomato    tone      tongue    tool      tooth     top
   topic     total     touch     tough     tour      towel
   tower     town      track     trade     train     trash
   travel    tray      treat     tree      trick     trip
   trout     trowel    truck     trupet    trust     truth
   try       tube      tuna      tune      turf      turkey
   turn      turnip    tutor     tux       tweet     twist
   two       type      union     unique    unit      upbeat
   upper     use       useful    user      usual     valley
   value     van       vase      vast      veil      vein
   velvet    verse     very      vessel    vest      video
   view      violet    visit     visual    vivid     voice
   volume    vowel     voyage    waffle    wait      wake
   walk      wall      warm      warmth    wasabi    wash
   watch     water     wave      wax       way       wealth
   wear      web       wedge     week      weekly    weight
   west      whale     what      wheat     wheel     when
   where     while     who       whole     why       will
   win       wind      window    wing      winner    winter
   wire      wish      witty     wolf      wonder    wood
   wool      woolly    word      work      worker    world
   worry     worth     worthy    wrap      wrench    wrist
   writer    xenon     yak       yam       yard      yarrow
   year      yearly    yellow    yew       yogurt    young
   youth     zebra     zephyr    zinc      zone      zoo
}
set nwordlist [llength $wordlist]

proc random_char {} {
  return [string index \
             "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" \
             [expr {int(rand()*52)}]]
}
proc random_label {} {
  set label [random_char]
  while {rand()>0.8} {
    append label [random_char]
  }
  if {rand()>0.9} {append label -}
  append label [format %d [expr {int(rand()*100)}]]
  return $label
}
proc random_numeric {} {
  set n [expr {(rand()*2-1.0)*1e6}]
  switch [expr {int(rand()*6)}] {
    0  {set format %.3f}
    1  {set format %.6E}
    2  {set format %.4e}
    default  {set format %g}
  }
  return [format $format $n]
}


proc random_json {limit indent} {
  global nwordlist wordlist
  set res {}
  if {$indent==0 || ($limit>0 && rand()>0.5)} {
    incr limit -1
    incr indent 2
    set n [expr {int(rand()*5)+1}]
    if {$n==5} {incr n [expr {int(rand()*10)}]}
    if {rand()>0.5} {
      set res \173\n
      for {set i 0} {$i<$n} {incr i} {
        append res [string repeat { } $indent]
        if {rand()>0.8} {
          if {rand()>0.5} {
            set sep ":\n   [string repeat { } $indent]"
          } else {
            set sep " : "
          }
        } else {
          set sep :
        }
        append res \"[random_label]\"$sep[random_json $limit $indent]
        if {$i<$n-1} {append res ,}
        append res \n
      }
      incr indent -2
      append res [string repeat { } $indent]
      append res \175
      return $res
    } else {
      set res \[\n
      for {set i 0} {$i<$n} {incr i} {
        append res [string repeat { } $indent]
        append res [random_json $limit $indent]
        if {$i<$n-1} {append res ,}
        append res \n
      }
      incr indent -2
      append res [string repeat { } $indent]
      append res \]
      return $res
    }
  } elseif {rand()>0.9} {
    if {rand()>0.7} {return "true"}
    if {rand()>0.5} {return "false"}
    return "null"
  } elseif {rand()>0.5} {
    return [random_numeric]
  } else {
    set res \"
    set n [expr {int(rand()*4)+1}]
    if {$n>=4} {set n [expr {$n+int(rand()*6)}]}
    for {set i 0} {$i<$n} {incr i} {
      if {rand()<0.05} {
        set w [random_numeric]
      } else {
        set k [expr {int(rand()*$nwordlist)}]
        set w [lindex $wordlist $k]
      }
      if {rand()<0.07} {
         set w \\\"$w\\\"
      }
      if {$i<$n-1} {
        switch [expr {int(rand()*9)}] {
          0       {set sp {, }}
          1       {set sp "\\n "}
          2       {set sp "-"}
          default {set sp { }}
        }
        append res $w$sp
      } else {
        append res $w
        if {rand()<0.2} {append res .}
      }
    }
    return $res\"
  }
}

puts "CREATE TABLE IF NOT EXISTS data1(x JSON);"
puts "BEGIN;"
set sz 0
for {set i 0} {$sz<100000000} {incr i} {
  set j [random_json 7 0]
  incr sz [string length $j]
  puts "INSERT INTO data1(x) VALUES('$j');"
}
puts "COMMIT;"
puts "SELECT sum(length(x)) FROM data1;"
